import { type ExportResult, ExportResultCode } from "@opentelemetry/core";
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-base";
import { type SDKOptions, Polar as Spaire } from "@spaire/sdk";
import type { EventCreateCustomer } from "@spaire/sdk/models/components/eventcreatecustomer.js";
import type { EventCreateExternalCustomer } from "@spaire/sdk/models/components/eventcreateexternalcustomer.js";
import type { EventMetadataInput } from "@spaire/sdk/models/components/eventmetadatainput.js";

const convertAttributesToMetadata = (
	attributes: ReadableSpan["attributes"],
): Record<string, EventMetadataInput> => {
	return Object.entries(attributes).reduce(
		(acc, [key, value]) => {
			if (typeof value === "string") {
				acc[key] = value;
			} else if (typeof value === "number") {
				acc[key] = value;
			} else if (typeof value === "boolean") {
				acc[key] = value;
			} else if (value instanceof Date) {
				acc[key] = value.toISOString();
			}

			return acc;
		},
		{} as Record<string, EventMetadataInput>,
	);
};

const customerIdKey = "customerId" as const;
const externalCustomerIdKey = "externalCustomerId" as const;

const convertSpanToSpaireEvent = (
	span: ReadableSpan,
): (EventCreateCustomer | EventCreateExternalCustomer) | null => {
	const customerId = span.attributes[customerIdKey];
	const externalCustomerId = span.attributes[externalCustomerIdKey];

	if (customerId && typeof customerId === "string") {
		return {
			name: span.name,
			customerId,
			metadata: convertAttributesToMetadata(span.attributes),
			parentId: span.parentSpanContext?.spanId,
			externalId: span.spanContext().spanId,
		};
	}

	if (externalCustomerId && typeof externalCustomerId === "string") {
		return {
			name: span.name,
			externalCustomerId,
			metadata: convertAttributesToMetadata(span.attributes),
			parentId: span.parentSpanContext?.spanId,
			externalId: span.spanContext().spanId,
		};
	}

	return null;
};

export class SpaireTraceExporter implements SpanExporter {
	constructor(private options: SDKOptions) {}

	async export(
		spans: ReadableSpan[],
		resultCallback: (result: ExportResult) => void,
	) {
		try {
			const spansWithCustomerId = spans.filter(
				(span) =>
					"customerId" in span.attributes ||
					"externalCustomerId" in span.attributes,
			);

			const payload = spansWithCustomerId
				.map(convertSpanToSpaireEvent)
				.filter((event) => event !== null);

			const spaire = new Spaire(this.options);

			await spaire.events.ingest({
				events: payload,
			});

			resultCallback({ code: ExportResultCode.SUCCESS });
		} catch (err) {
			resultCallback({ code: ExportResultCode.FAILED, error: err as Error });
		}
	}

	shutdown() {
		return Promise.resolve();
	}
}
