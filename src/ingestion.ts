import { type SDKOptions, Polar as Spaire } from "@spaire/sdk";
import type { CostMetadataInput } from "@spaire/sdk/models/components/costmetadatainput.js";
import type { EventCreateCustomer } from "@spaire/sdk/models/components/eventcreatecustomer.js";
import type { EventMetadataInput } from "@spaire/sdk/models/components/eventmetadatainput.js";
import type {
	IngestionStrategy,
	IngestionStrategyContext,
	IngestionStrategyCustomer,
	IngestionStrategyExternalCustomer,
} from "./strategy";

export type IngestionContext<
	TContext extends Record<string, EventMetadataInput> = Record<
		string,
		EventMetadataInput
	>,
> = TContext;

type Transformer<TContext extends IngestionContext> = (
	ctx: TContext,
	customer: IngestionStrategyCustomer | IngestionStrategyExternalCustomer,
) => Promise<void>;

export type Span = {
	name: string;
	startTime: number;
	endTime: number;
};

export class SpaireIngestion<TContext extends IngestionContext> {
	public spaireClient?: Spaire;
	private transformers: Transformer<TContext>[] = [];
	public costResolver?: (ctx: TContext) => CostMetadataInput;
	public span?: Span;

	private pipe(transformer: Transformer<TContext>) {
		this.transformers.push(transformer);

		return this;
	}

	public async execute(
		ctx: TContext,
		customer: IngestionStrategyCustomer | IngestionStrategyExternalCustomer,
	) {
		await Promise.all(
			this.transformers.map((transformer) => transformer(ctx, customer)),
		);
	}

	public schedule(
		meter: string,
		metadataResolver?: (ctx: TContext) => Record<string, EventMetadataInput>,
	) {
		return this.pipe(async (ctx, customer) => {
			if (!this.spaireClient) {
				throw new Error("Spaire client not initialized");
			}

			await this.spaireClient.events.ingest({
				events: [
					{
						...customer,
						name: meter,
						metadata: {
							...(metadataResolver ? metadataResolver(ctx) : ctx),
							...(this.costResolver ? { _cost: this.costResolver(ctx) } : {}),
						},
					},
				],
			});
		});
	}
}

export function Ingestion(spaireConfig?: SDKOptions) {
	return {
		strategy: <TContext extends IngestionStrategyContext, TStrategyClient>(
			strategy: IngestionStrategy<TContext, TStrategyClient>,
		) => {
			strategy.spaireClient = new Spaire(spaireConfig);
			return strategy;
		},
		ingest: async (events: (EventCreateCustomer | EventCreateCustomer)[]) => {
			const spaire = new Spaire(spaireConfig);

			return spaire.events.ingest({
				events,
			});
		},
	};
}
