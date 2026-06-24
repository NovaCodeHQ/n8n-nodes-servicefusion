import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeProperties,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	ICredentialTestFunctions,
	INodeCredentialTestResult,
	ICredentialsDecrypted,
	ICredentialDataDecryptedObject,
	JsonObject,
} from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError, NodeApiError } from 'n8n-workflow';
import { ApplicationError } from '@n8n/errors';

import { ServiceFusionAdapter } from './vendor/servicefusion-adapter.bundle';

import { createAdapter, disconnectAdapter, getAdapterDebugState } from './GenericFunctions';

const RESOURCES = [
	'customer',
	'job',
	'estimate',
	'invoice',
	'technician',
	'jobCategory',
	'jobStatus',
	'paymentType',
	'source',
	'calendarTask',
	'webhook',
] as const;
type Resource = (typeof RESOURCES)[number];

const RESOURCE_LABELS: Record<Resource, string> = {
	customer: 'Customer',
	job: 'Job',
	estimate: 'Estimate',
	invoice: 'Invoice',
	technician: 'Technician',
	jobCategory: 'Job Category',
	jobStatus: 'Job Status',
	paymentType: 'Payment Type',
	source: 'Source',
	calendarTask: 'Calendar Task',
	webhook: 'Webhook',
};

const OPERATIONS: Record<Resource, string[]> = {
	customer: ['getAll', 'get', 'create', 'update', 'delete', 'search'],
	job: ['getAll', 'get', 'create', 'update', 'delete', 'search', 'getAllPaged', 'batchSync'],
	estimate: ['getAll', 'get', 'create', 'update', 'convertToJob', 'search'],
	invoice: ['getAll', 'get', 'create', 'update', 'send'],
	technician: ['getAll', 'get', 'getSchedule', 'assignJob'],
	jobCategory: ['getAll', 'get'],
	jobStatus: ['getAll', 'get'],
	paymentType: ['getAll', 'get'],
	source: ['getAll', 'get'],
	calendarTask: ['getAll', 'get'],
	webhook: ['create', 'delete'],
};

type ServiceFusionError = Error & {
	statusCode?: number;
	code?: string;
	details?: unknown;
};

const GENERIC_NETWORK_ERROR_PATTERNS = [
	'ECONNREFUSED',
	'ECONNRESET',
	'ENOTFOUND',
	'ETIMEDOUT',
	'EHOSTUNREACH',
	'EAI_AGAIN',
	'ERR_NETWORK',
];

function formatDebugValue(value: unknown): string | undefined {
	if (value === undefined || value === null) {
		return undefined;
	}

	if (typeof value === 'string') {
		return value;
	}

	try {
		return JSON.stringify(value, null, 2);
	} catch {
		return String(value);
	}
}

function getApiErrorDetails(error: unknown, adapter: ServiceFusionAdapter | null = null) {
	if (!(error instanceof Error)) {
		return null;
	}

	const serviceFusionError = error as ServiceFusionError;
	const debugState = getAdapterDebugState(adapter);
	const descriptionParts: string[] = [];
	const requestMethod = debugState?.lastRequest?.method?.toUpperCase();
	const requestUrl = debugState?.lastRequest?.url;
	const responseStatus = serviceFusionError.statusCode ?? debugState?.lastResponse?.status;
	const responseBody = formatDebugValue(serviceFusionError.details ?? debugState?.lastAuthError);
	const rawMessage = serviceFusionError.message || 'ServiceFusion API request failed';
	const hasGenericNetworkPattern = GENERIC_NETWORK_ERROR_PATTERNS.some((pattern) =>
		rawMessage.toUpperCase().includes(pattern),
	);
	const message =
		responseStatus !== undefined
			? `ServiceFusion API request failed with HTTP ${responseStatus}`
			: hasGenericNetworkPattern
				? 'ServiceFusion API connection failed'
				: rawMessage;

	if (requestMethod || requestUrl) {
		descriptionParts.push(`Request: ${requestMethod ?? 'GET'} ${requestUrl ?? ''}`.trim());
	}

	if (responseStatus !== undefined) {
		descriptionParts.push(`HTTP status: ${responseStatus}`);
	}

	if (serviceFusionError.code) {
		descriptionParts.push(`ServiceFusion code: ${serviceFusionError.code}`);
	}

	if (responseBody) {
		descriptionParts.push(`Response body: ${responseBody}`);
	}

	if (message !== rawMessage) {
		descriptionParts.push(`Original error: ${rawMessage}`);
	}

	return {
		message,
		description: descriptionParts.join('\n\n') || undefined,
		httpCode: responseStatus !== undefined ? String(responseStatus) : undefined,
	};
}

function toItemArray(response: unknown): IDataObject[] {
	if (Array.isArray(response)) {
		return response as IDataObject[];
	}

	if (response && typeof response === 'object') {
		const data = response as { items?: unknown };
		if (Array.isArray(data.items)) {
			return data.items as IDataObject[];
		}
	}

	throw new ApplicationError(
		`Expected list response but received: ${formatDebugValue(response) ?? 'unknown value'}`,
	);
}

function mapListResponse(response: unknown): INodeExecutionData[] {
	return toItemArray(response).map((item) => ({ json: item }));
}

function formatDateOnly(value: string): string {
	return new Date(value).toISOString().slice(0, 10);
}

function formatDateTime(value: string): string {
	return new Date(value).toISOString();
}

type ListRequestAdapter = ServiceFusionAdapter & {
	request: (config: {
		method: string;
		params?: Record<string, number | string>;
		url: string;
	}) => Promise<unknown>;
};

type SimpleListResourceConfig = {
	endpoint: string;
	idParam: string;
	limitParam: string;
	offsetParam: string;
};

function addSimpleListResourceProperties(
	props: INodeProperties[],
	resource: Resource,
	idDisplayName: string,
	idParam: string,
	limitParam: string,
	offsetParam: string,
) {
	props.push({
		displayName: 'Limit',
		name: limitParam,
		type: 'number',
		typeOptions: { minValue: 1 },
		default: 100,
		description: 'Max number of results to return',
		displayOptions: { show: { resource: [resource], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Offset',
		name: offsetParam,
		type: 'number',
		default: 0,
		displayOptions: { show: { resource: [resource], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: idDisplayName,
		name: idParam,
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [resource], operation: ['get'] } },
	} as INodeProperties);
}

async function executeSimpleListResource(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
	config: SimpleListResourceConfig,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	const requestAdapter = adapter as ListRequestAdapter;
	switch (operation) {
		case 'getAll': {
			const limit = p(config.limitParam) as number;
			const offset = p(config.offsetParam) as number;
			const params: Record<string, number> = {};
			if (limit > 0) {
				params['per-page'] = limit;
			}
			if (offset >= 0) {
				params.page = limit > 0 ? Math.floor(offset / limit) + 1 : offset + 1;
			}
			const r = await requestAdapter.request({
				method: 'GET',
				url: config.endpoint,
				params,
			});
			return mapListResponse(r);
		}
		case 'get': {
			const id = p(config.idParam) as string;
			const r = await requestAdapter.request({
				method: 'GET',
				url: `${config.endpoint}/${id}`,
			});
			return [{ json: r as unknown as IDataObject }];
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown operation: ${operation}`);
	}
}

function allProperties(): INodeProperties[] {
	const props: INodeProperties[] = [];
	props.push({
		displayName: 'Resource',
		name: 'resource',
		type: 'options',
		noDataExpression: true,
		options: RESOURCES.map((r) => ({ name: RESOURCE_LABELS[r], value: r })),
		default: 'customer',
	});
	props.push(
		...(RESOURCES.map((resource) => ({
			displayName: 'Operation',
			name: 'operation',
			type: 'options' as const,
			noDataExpression: true,
			displayOptions: { show: { resource: [resource] } },
			options: OPERATIONS[resource].map((op) => ({
				name: op
					.replace(/([A-Z])/g, ' $1')
					.replace(/^./, (s) => s.toUpperCase())
					.trim(),
				value: op,
			})),
			default: OPERATIONS[resource][0],
		})) as unknown as INodeProperties[]),
	);
	const C = 'customer',
		J = 'job',
		E = 'estimate',
		I = 'invoice',
		T = 'technician',
		JC = 'jobCategory',
		JS = 'jobStatus',
		PT = 'paymentType',
		S = 'source',
		CT = 'calendarTask',
		W = 'webhook';
	props.push({
		displayName: 'Customer ID',
		name: 'customerId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [C], operation: ['get'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Limit',
		name: 'limit',
		type: 'number',
		typeOptions: {
			minValue: 1,
		},
		description: 'Max number of results to return',
		default: 50,
		displayOptions: { show: { resource: [C], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Offset',
		name: 'offset',
		type: 'number',
		default: 0,
		displayOptions: { show: { resource: [C], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer Name',
		name: 'customerName',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Type',
		name: 'customerType',
		type: 'options',
		default: 'Residential',
		options: [
			{ name: 'Residential', value: 'Residential' },
			{ name: 'Commercial', value: 'Commercial' },
		],
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Email',
		name: 'customerEmail',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Phone',
		name: 'customerPhone',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Mobile',
		name: 'customerMobile',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Street Address',
		name: 'customerStreet1',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'City',
		name: 'customerCity',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'State',
		name: 'customerState',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Zip Code',
		name: 'customerZipCode',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Additional Fields',
		name: 'additionalCustomerFields',
		type: 'collection',
		default: {},
		displayOptions: { show: { resource: [C], operation: ['create'] } },
		options: [
			{ displayName: 'Notes', name: 'notes', type: 'string', default: '' },
			{
				displayName: 'Tags',
				name: 'tags',
				type: 'string',
				default: '',
				placeholder: 'Comma-separated',
			},
			{ displayName: 'External ID', name: 'externalId', type: 'string', default: '' },
		],
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'customerId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [C], operation: ['update', 'delete'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer Name',
		name: 'customerName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Notes',
		name: 'customerNotes',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Name',
		name: 'searchName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Email',
		name: 'searchEmail',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Phone',
		name: 'searchPhone',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'City',
		name: 'searchCity',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'State',
		name: 'searchState',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Zip Code',
		name: 'searchZipCode',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Type',
		name: 'searchType',
		type: 'options',
		default: '',
		options: [
			{ name: 'All', value: '' },
			{ name: 'Residential', value: 'Residential' },
			{ name: 'Commercial', value: 'Commercial' },
		],
		displayOptions: { show: { resource: [C], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'jobId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [J], operation: ['get'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'jobCustomerId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Status',
		name: 'jobStatus',
		type: 'multiOptions',
		default: [],
		options: [
			{ name: 'Cancelled', value: 'Cancelled' },
			{ name: 'Completed', value: 'Completed' },
			{ name: 'In Progress', value: 'In Progress' },
			{ name: 'On Hold', value: 'On Hold' },
			{ name: 'Open', value: 'Open' },
			{ name: 'Scheduled', value: 'Scheduled' },
		],
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Priority',
		name: 'jobPriority',
		type: 'multiOptions',
		default: [],
		options: [
			{ name: 'Low', value: 'Low' },
			{ name: 'Medium', value: 'Medium' },
			{ name: 'High', value: 'High' },
			{ name: 'Emergency', value: 'Emergency' },
		],
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Technician ID',
		name: 'jobTechnicianId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Scheduled Date From',
		name: 'scheduledDateFrom',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Scheduled Date To',
		name: 'scheduledDateTo',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['getAll', 'search', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Limit',
		name: 'limit',
		type: 'number',
		typeOptions: {
			minValue: 1,
		},
		description: 'Max number of results to return',
		default: 50,
		displayOptions: { show: { resource: [J], operation: ['getAll', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Offset',
		name: 'offset',
		type: 'number',
		default: 0,
		displayOptions: { show: { resource: [J], operation: ['getAll', 'getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Page Size',
		name: 'pageSize',
		type: 'number',
		default: 100,
		displayOptions: { show: { resource: [J], operation: ['getAllPaged'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'jobCustomerId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job Type',
		name: 'jobType',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Priority',
		name: 'jobPriority',
		type: 'options',
		default: 'Medium',
		options: [
			{ name: 'Low', value: 'Low' },
			{ name: 'Medium', value: 'Medium' },
			{ name: 'High', value: 'High' },
			{ name: 'Emergency', value: 'Emergency' },
		],
		displayOptions: { show: { resource: [J], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Status',
		name: 'jobStatus',
		type: 'options',
		default: 'Open',
		options: [
			{ name: 'Cancelled', value: 'Cancelled' },
			{ name: 'Completed', value: 'Completed' },
			{ name: 'In Progress', value: 'In Progress' },
			{ name: 'On Hold', value: 'On Hold' },
			{ name: 'Open', value: 'Open' },
			{ name: 'Scheduled', value: 'Scheduled' },
		],
		displayOptions: { show: { resource: [J], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Description',
		name: 'jobDescription',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Scheduled Date',
		name: 'scheduledDate',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Assigned Technician ID',
		name: 'jobAssignedTo',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Street Address',
		name: 'jobStreet1',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'City',
		name: 'jobCity',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'State',
		name: 'jobState',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Zip Code',
		name: 'jobZipCode',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Additional Fields',
		name: 'additionalJobFields',
		type: 'collection',
		default: {},
		displayOptions: { show: { resource: [J], operation: ['create'] } },
		options: [
			{ displayName: 'Notes', name: 'notes', type: 'string', default: '' },
			{ displayName: 'Internal Notes', name: 'internalNotes', type: 'string', default: '' },
			{
				displayName: 'Estimated Duration (Min)',
				name: 'estimatedDuration',
				type: 'number',
				default: 0,
			},
		],
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'jobId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [J], operation: ['update', 'delete'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Notes',
		name: 'jobNotes',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'External ID',
		name: 'searchExternalId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Description (text search)',
		name: 'searchDescription',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [J], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Jobs (JSON Array)',
		name: 'jobsJson',
		type: 'json',
		default: '',
		typeOptions: { alwaysOpenEditWindow: true },
		displayOptions: { show: { resource: [J], operation: ['batchSync'] } },
		description: 'Array of job objects to sync',
	} as INodeProperties);
	props.push({
		displayName: 'Match By',
		name: 'matchBy',
		type: 'options',
		default: 'externalId',
		options: [
			{ name: 'External ID', value: 'externalId' },
			{ name: 'Customer ID', value: 'customerId' },
			{ name: 'Description', value: 'description' },
		],
		displayOptions: { show: { resource: [J], operation: ['batchSync'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Update Existing',
		name: 'updateExisting',
		type: 'options',
		default: 'true',
		options: [
			{ name: 'True', value: 'true' },
			{ name: 'False', value: 'false' },
		],
		displayOptions: { show: { resource: [J], operation: ['batchSync'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Create New',
		name: 'createNew',
		type: 'options',
		default: 'true',
		options: [
			{ name: 'True', value: 'true' },
			{ name: 'False', value: 'false' },
		],
		displayOptions: { show: { resource: [J], operation: ['batchSync'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Estimate ID',
		name: 'estimateId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [E], operation: ['get', 'update', 'convertToJob'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID (optional filter)',
		name: 'estimateJobId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['getAll', 'search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Limit',
		name: 'estimateLimit',
		type: 'number',
		typeOptions: {
			minValue: 1,
		},
		description: 'Max number of results to return',
		default: 50,
		displayOptions: { show: { resource: [E], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Offset',
		name: 'estimateOffset',
		type: 'number',
		default: 0,
		displayOptions: { show: { resource: [E], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Status',
		name: 'estimateSearchStatus',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Number',
		name: 'estimateSearchNumber',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'PO Number',
		name: 'estimateSearchPoNumber',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer Name',
		name: 'estimateSearchCustomerName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Parent Customer Name',
		name: 'estimateSearchParentCustomerName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Contact First Name',
		name: 'estimateSearchContactFirstName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Contact Last Name',
		name: 'estimateSearchContactLastName',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Address',
		name: 'estimateSearchAddress',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'City',
		name: 'estimateSearchCity',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Zip Code',
		name: 'estimateSearchZipCode',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Phone',
		name: 'estimateSearchPhone',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Email',
		name: 'estimateSearchEmail',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Category',
		name: 'estimateSearchCategory',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Source',
		name: 'estimateSearchSource',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Start Date From',
		name: 'estimateSearchStartDateFrom',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Start Date To',
		name: 'estimateSearchStartDateTo',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'End Date From',
		name: 'estimateSearchEndDateFrom',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'End Date To',
		name: 'estimateSearchEndDateTo',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Requested Date From',
		name: 'estimateSearchRequestedDateFrom',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Requested Date To',
		name: 'estimateSearchRequestedDateTo',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['search'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'estimateCustomerId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [E], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'estimateJobId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Description',
		name: 'estimateDescription',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [E], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Invoice ID',
		name: 'invoiceId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [I], operation: ['get', 'update', 'send'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'invoiceCustomerId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'invoiceJobId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Status',
		name: 'invoiceStatus',
		type: 'multiOptions',
		default: [],
		options: [
			{ name: 'Cancelled', value: 'Cancelled' },
			{ name: 'Draft', value: 'Draft' },
			{ name: 'Overdue', value: 'Overdue' },
			{ name: 'Paid', value: 'Paid' },
			{ name: 'Partial', value: 'Partial' },
			{ name: 'Sent', value: 'Sent' },
		],
		displayOptions: { show: { resource: [I], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Date From',
		name: 'invoiceDateFrom',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Date To',
		name: 'invoiceDateTo',
		type: 'dateTime',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Customer ID',
		name: 'invoiceCustomerId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [I], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'invoiceJobId',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Description',
		name: 'invoiceDescription',
		type: 'string',
		default: '',
		displayOptions: { show: { resource: [I], operation: ['create', 'update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Update Status',
		name: 'invoiceUpdateStatus',
		type: 'options',
		default: 'Draft',
		options: [
			{ name: 'Cancelled', value: 'Cancelled' },
			{ name: 'Draft', value: 'Draft' },
			{ name: 'Overdue', value: 'Overdue' },
			{ name: 'Paid', value: 'Paid' },
			{ name: 'Partial', value: 'Partial' },
			{ name: 'Sent', value: 'Sent' },
		],
		displayOptions: { show: { resource: [I], operation: ['update'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Send To Email',
		name: 'sendToEmail',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [I], operation: ['send'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Technician ID',
		name: 'technicianId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [T], operation: ['get', 'getSchedule'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Limit',
		name: 'technicianLimit',
		type: 'number',
		default: 100,
		displayOptions: { show: { resource: [T], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Offset',
		name: 'technicianOffset',
		type: 'number',
		default: 0,
		displayOptions: { show: { resource: [T], operation: ['getAll'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Schedule Date',
		name: 'scheduleDate',
		type: 'dateTime',
		default: '',
		required: true,
		displayOptions: { show: { resource: [T], operation: ['getSchedule'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Job ID',
		name: 'assignJobId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [T], operation: ['assignJob'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Technician ID',
		name: 'assignTechnicianId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [T], operation: ['assignJob'] } },
	} as INodeProperties);
	props.push({
		displayName: 'URL',
		name: 'webhookUrl',
		type: 'string',
		default: '',
		required: true,
		description: 'Webhook callback URL',
		displayOptions: { show: { resource: [W], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Events (JSON Array)',
		name: 'webhookEvents',
		type: 'json',
		default: '',
		typeOptions: { alwaysOpenEditWindow: true },
		displayOptions: { show: { resource: [W], operation: ['create'] } },
		description: 'Array of event strings',
	} as INodeProperties);
	props.push({
		displayName: 'Secret',
		name: 'webhookSecret',
		type: 'string',
		typeOptions: { password: true },
		default: '',
		displayOptions: { show: { resource: [W], operation: ['create'] } },
	} as INodeProperties);
	props.push({
		displayName: 'Webhook ID',
		name: 'webhookId',
		type: 'string',
		default: '',
		required: true,
		displayOptions: { show: { resource: [W], operation: ['delete'] } },
	} as INodeProperties);
	addSimpleListResourceProperties(
		props,
		JC,
		'Job Category ID',
		'jobCategoryId',
		'jobCategoryLimit',
		'jobCategoryOffset',
	);
	addSimpleListResourceProperties(
		props,
		JS,
		'Job Status ID',
		'jobStatusId',
		'jobStatusLimit',
		'jobStatusOffset',
	);
	addSimpleListResourceProperties(
		props,
		PT,
		'Payment Type ID',
		'paymentTypeId',
		'paymentTypeLimit',
		'paymentTypeOffset',
	);
	addSimpleListResourceProperties(props, S, 'Source ID', 'sourceId', 'sourceLimit', 'sourceOffset');
	addSimpleListResourceProperties(
		props,
		CT,
		'Calendar Task ID',
		'calendarTaskId',
		'calendarTaskLimit',
		'calendarTaskOffset',
	);
	return props;
}

async function executeCustomer(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			const r = await adapter.getCustomers({
				limit: (p('limit') as number) || undefined,
				offset: (p('offset') as number) || undefined,
				sortBy: 'customer_name',
			});
			return mapListResponse(r);
		}
		case 'get': {
			const r = await adapter.getCustomer(p('customerId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'create': {
			const d: Record<string, unknown> = {};
			if (p('customerName')) d.name = p('customerName');
			if (p('customerType')) d.type = p('customerType');
			if (p('customerEmail')) d.email = p('customerEmail');
			if (p('customerPhone')) d.phone = p('customerPhone');
			if (p('customerMobile')) d.mobile = p('customerMobile');
			if (p('customerStreet1') || p('customerCity')) {
				d.address = {
					street1: (p('customerStreet1') as string) ?? '',
					city: (p('customerCity') as string) ?? '',
					state: (p('customerState') as string) ?? '',
					zipCode: (p('customerZipCode') as string) ?? '',
				};
			}
			const a = p('additionalCustomerFields', {}) as Record<string, unknown>;
			if (a.notes) d.notes = a.notes;
			if (a.tags) d.tags = (a.tags as string).split(',').map((s: string) => s.trim());
			if (a.externalId) d.externalId = a.externalId;
			const r = await adapter.createCustomer(d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'update': {
			const id = p('customerId') as string;
			const d: Record<string, unknown> = {};
			if (p('customerName')) d.name = p('customerName');
			if (p('customerEmail')) d.email = p('customerEmail');
			if (p('customerPhone')) d.phone = p('customerPhone');
			if (p('customerNotes')) d.notes = p('customerNotes');
			const r = await adapter.updateCustomer(id, d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'delete': {
			const id = p('customerId') as string;
			await adapter.deleteCustomer(id);
			return [{ json: { success: true, customerId: id } }];
		}
		case 'search': {
			const f: Record<string, unknown> = {};
			if (p('searchName')) f.name = p('searchName');
			if (p('searchEmail')) f.email = p('searchEmail');
			if (p('searchPhone')) f.phone = p('searchPhone');
			if (p('searchCity')) f.city = p('searchCity');
			if (p('searchState')) f.state = p('searchState');
			if (p('searchZipCode')) f.zipCode = p('searchZipCode');
			if (p('searchType')) f.type = p('searchType');
			const r = await adapter.searchCustomers(f);
			return mapListResponse(r);
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown customer operation: ${operation}`);
	}
}

async function executeJob(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			const f: Record<string, unknown> = {};
			if (p('jobCustomerId')) f.customerId = p('jobCustomerId');
			const js = p('jobStatus');
			if (Array.isArray(js) && js.length) f.status = js;
			const jp = p('jobPriority');
			if (Array.isArray(jp) && jp.length) f.priority = jp;
			if (p('jobTechnicianId')) f.technicianId = p('jobTechnicianId');
			if (p('scheduledDateFrom')) f.scheduledDateFrom = new Date(p('scheduledDateFrom') as string);
			if (p('scheduledDateTo')) f.scheduledDateTo = new Date(p('scheduledDateTo') as string);
			if (p('limit')) f.limit = p('limit');
			if (p('offset')) f.offset = p('offset');
			const r = await adapter.getJobs(f);
			return mapListResponse(r);
		}
		case 'get': {
			const r = await adapter.getJob(p('jobId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'create': {
			const d: Record<string, unknown> = {};
			if (p('jobCustomerId')) d.customerId = p('jobCustomerId');
			if (p('jobType')) d.jobType = p('jobType');
			if (p('jobPriority')) d.priority = p('jobPriority');
			if (p('jobStatus')) d.status = p('jobStatus');
			if (p('jobDescription')) d.description = p('jobDescription');
			if (p('scheduledDate')) d.scheduledDate = new Date(p('scheduledDate') as string);
			if (p('jobAssignedTo')) d.assignedTo = p('jobAssignedTo');
			if (p('jobStreet1') || p('jobCity')) {
				d.address = {
					street1: (p('jobStreet1') as string) ?? '',
					city: (p('jobCity') as string) ?? '',
					state: (p('jobState') as string) ?? '',
					zipCode: (p('jobZipCode') as string) ?? '',
				};
			}
			const a = p('additionalJobFields', {}) as Record<string, unknown>;
			if (a.notes) d.notes = a.notes;
			if (a.internalNotes) d.internalNotes = a.internalNotes;
			if (a.estimatedDuration) d.estimatedDuration = a.estimatedDuration;
			const r = await adapter.createJob(d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'update': {
			const id = p('jobId') as string;
			const d: Record<string, unknown> = {};
			if (p('jobType')) d.jobType = p('jobType');
			if (p('jobPriority')) d.priority = p('jobPriority');
			if (p('jobStatus')) d.status = p('jobStatus');
			if (p('jobDescription')) d.description = p('jobDescription');
			if (p('jobNotes')) d.notes = p('jobNotes');
			const r = await adapter.updateJob(id, d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'delete': {
			const id = p('jobId') as string;
			await adapter.deleteJob(id);
			return [{ json: { success: true, jobId: id } }];
		}
		case 'search': {
			const f: Record<string, unknown> = {};
			if (p('jobCustomerId')) f.customerId = p('jobCustomerId');
			const js = p('jobStatus');
			if (Array.isArray(js) && js.length) f.status = js;
			const jp = p('jobPriority');
			if (Array.isArray(jp) && jp.length) f.priority = jp;
			if (p('jobTechnicianId')) f.technicianId = p('jobTechnicianId');
			if (p('scheduledDateFrom')) f.scheduledDateFrom = new Date(p('scheduledDateFrom') as string);
			if (p('scheduledDateTo')) f.scheduledDateTo = new Date(p('scheduledDateTo') as string);
			if (p('searchExternalId')) f.externalId = p('searchExternalId');
			if (p('searchDescription')) f.description = p('searchDescription');
			const r = await adapter.searchJobs(f);
			return mapListResponse(r);
		}
		case 'getAllPaged': {
			const f: Record<string, unknown> & { pageSize?: number } = {};
			if (p('jobCustomerId')) f.customerId = p('jobCustomerId');
			const js = p('jobStatus');
			if (Array.isArray(js) && js.length) f.status = js;
			const jp = p('jobPriority');
			if (Array.isArray(jp) && jp.length) f.priority = jp;
			if (p('jobTechnicianId')) f.technicianId = p('jobTechnicianId');
			if (p('scheduledDateFrom')) f.scheduledDateFrom = new Date(p('scheduledDateFrom') as string);
			if (p('scheduledDateTo')) f.scheduledDateTo = new Date(p('scheduledDateTo') as string);
			if (p('pageSize')) f.pageSize = p('pageSize') as number;
			const r = await adapter.getAllJobs(f);
			return mapListResponse(r);
		}
		case 'batchSync': {
			const raw = p('jobsJson') as string;
			let jobs: unknown[];
			try {
				jobs = JSON.parse(raw);
			} catch {
				throw new NodeApiError(ctx.getNode(), { message: 'Invalid JSON in Jobs field' });
			}
			const r = await adapter.batchSyncJobs(jobs as never, {
				matchBy: p('matchBy') as string as 'externalId' | 'customerId' | 'description',
				updateExisting: (p('updateExisting') as string) === 'true',
				createNew: (p('createNew') as string) === 'true',
			});
			return [{ json: r as unknown as IDataObject }];
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown job operation: ${operation}`);
	}
}

async function executeEstimate(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			const requestAdapter = adapter as ServiceFusionAdapter & {
				request: (config: {
					method: string;
					params?: Record<string, number>;
					url: string;
				}) => Promise<unknown>;
			};
			const jobId = (p('estimateJobId') as string) || undefined;
			const limit = p('estimateLimit') as number;
			const offset = p('estimateOffset') as number;
			const params: Record<string, number> = {};
			if (limit > 0) {
				params['per-page'] = limit;
			}
			if (offset >= 0) {
				params.page = limit > 0 ? Math.floor(offset / limit) + 1 : offset + 1;
			}
			const r = await requestAdapter.request({
				method: 'GET',
				url: jobId ? `/jobs/${jobId}/estimates` : '/estimates',
				params,
			});
			return mapListResponse(r);
		}
		case 'get': {
			const r = await adapter.getEstimate(p('estimateId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'create': {
			const d: Record<string, unknown> = {};
			if (p('estimateCustomerId')) d.customerId = p('estimateCustomerId');
			if (p('estimateJobId')) d.jobId = p('estimateJobId');
			if (p('estimateDescription')) d.description = p('estimateDescription');
			const r = await adapter.createEstimate(d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'update': {
			const id = p('estimateId') as string;
			const d: Record<string, unknown> = {};
			if (p('estimateDescription')) d.description = p('estimateDescription');
			const r = await adapter.updateEstimate(id, d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'convertToJob': {
			const r = await adapter.convertEstimateToJob(p('estimateId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'search': {
			const requestAdapter = adapter as ServiceFusionAdapter & {
				request: (config: {
					method: string;
					params?: Record<string, string>;
					url: string;
				}) => Promise<unknown>;
			};
			const jobId = p('estimateJobId') as string;
			const params: Record<string, string> = {};
			if (p('estimateSearchStatus'))
				params['filters[status]'] = p('estimateSearchStatus') as string;
			if (p('estimateSearchNumber'))
				params['filters[number]'] = p('estimateSearchNumber') as string;
			if (p('estimateSearchPoNumber'))
				params['filters[po_number]'] = p('estimateSearchPoNumber') as string;
			if (p('estimateSearchCustomerName'))
				params['filters[customer_name]'] = p('estimateSearchCustomerName') as string;
			if (p('estimateSearchParentCustomerName'))
				params['filters[parent_customer_name]'] = p('estimateSearchParentCustomerName') as string;
			if (p('estimateSearchContactFirstName'))
				params['filters[contact_first_name]'] = p('estimateSearchContactFirstName') as string;
			if (p('estimateSearchContactLastName'))
				params['filters[contact_last_name]'] = p('estimateSearchContactLastName') as string;
			if (p('estimateSearchAddress'))
				params['filters[address]'] = p('estimateSearchAddress') as string;
			if (p('estimateSearchCity')) params['filters[city]'] = p('estimateSearchCity') as string;
			if (p('estimateSearchZipCode'))
				params['filters[zip_code]'] = p('estimateSearchZipCode') as string;
			if (p('estimateSearchPhone')) params['filters[phone]'] = p('estimateSearchPhone') as string;
			if (p('estimateSearchEmail')) params['filters[email]'] = p('estimateSearchEmail') as string;
			if (p('estimateSearchCategory'))
				params['filters[category]'] = p('estimateSearchCategory') as string;
			if (p('estimateSearchSource'))
				params['filters[source]'] = p('estimateSearchSource') as string;
			if (p('estimateSearchStartDateFrom'))
				params['filters[start_date][gte]'] = formatDateOnly(
					p('estimateSearchStartDateFrom') as string,
				);
			if (p('estimateSearchStartDateTo'))
				params['filters[start_date][lte]'] = formatDateOnly(
					p('estimateSearchStartDateTo') as string,
				);
			if (p('estimateSearchEndDateFrom'))
				params['filters[end_date][gte]'] = formatDateOnly(p('estimateSearchEndDateFrom') as string);
			if (p('estimateSearchEndDateTo'))
				params['filters[end_date][lte]'] = formatDateOnly(p('estimateSearchEndDateTo') as string);
			if (p('estimateSearchRequestedDateFrom'))
				params['filters[requested_date][gte]'] = formatDateTime(
					p('estimateSearchRequestedDateFrom') as string,
				);
			if (p('estimateSearchRequestedDateTo'))
				params['filters[requested_date][lte]'] = formatDateTime(
					p('estimateSearchRequestedDateTo') as string,
				);
			const r = await requestAdapter.request({
				method: 'GET',
				url: jobId ? `/jobs/${jobId}/estimates` : '/estimates',
				params,
			});
			return mapListResponse(r);
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown estimate operation: ${operation}`);
	}
}

async function executeInvoice(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			const f: Record<string, unknown> = {};
			if (p('invoiceCustomerId')) f.customerId = p('invoiceCustomerId');
			if (p('invoiceJobId')) f.jobId = p('invoiceJobId');
			const s = p('invoiceStatus');
			if (Array.isArray(s) && s.length) f.status = s;
			if (p('invoiceDateFrom')) f.dateFrom = new Date(p('invoiceDateFrom') as string);
			if (p('invoiceDateTo')) f.dateTo = new Date(p('invoiceDateTo') as string);
			const r = await adapter.getInvoices(f);
			return mapListResponse(r);
		}
		case 'get': {
			const r = await adapter.getInvoice(p('invoiceId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'create': {
			const d: Record<string, unknown> = {};
			if (p('invoiceCustomerId')) d.customerId = p('invoiceCustomerId');
			if (p('invoiceJobId')) d.jobId = p('invoiceJobId');
			if (p('invoiceDescription')) d.description = p('invoiceDescription');
			const r = await adapter.createInvoice(d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'update': {
			const id = p('invoiceId') as string;
			const d: Record<string, unknown> = {};
			if (p('invoiceDescription')) d.description = p('invoiceDescription');
			if (p('invoiceUpdateStatus')) d.status = p('invoiceUpdateStatus');
			const r = await adapter.updateInvoice(id, d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'send': {
			const id = p('invoiceId') as string;
			const email = p('sendToEmail') as string;
			await adapter.sendInvoice(id, email);
			return [{ json: { success: true, invoiceId: id, sentTo: email } }];
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown invoice operation: ${operation}`);
	}
}

async function executeTechnician(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			const r = await adapter.getTechnicians({
				limit: (p('technicianLimit') as number) || undefined,
				offset: (p('technicianOffset') as number) || undefined,
			});
			return mapListResponse(r);
		}
		case 'get': {
			const r = await adapter.getTechnician(p('technicianId') as string);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'getSchedule': {
			const r = await adapter.getSchedule(
				p('technicianId') as string,
				new Date(p('scheduleDate') as string),
			);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'assignJob': {
			const r = await adapter.assignJob(
				p('assignJobId') as string,
				p('assignTechnicianId') as string,
			);
			return [{ json: r as unknown as IDataObject }];
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown technician operation: ${operation}`);
	}
}

async function executeJobCategory(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	return executeSimpleListResource(ctx, adapter, operation, itemIndex, {
		endpoint: '/job-categories',
		idParam: 'jobCategoryId',
		limitParam: 'jobCategoryLimit',
		offsetParam: 'jobCategoryOffset',
	});
}

async function executeJobStatus(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	return executeSimpleListResource(ctx, adapter, operation, itemIndex, {
		endpoint: '/job-statuses',
		idParam: 'jobStatusId',
		limitParam: 'jobStatusLimit',
		offsetParam: 'jobStatusOffset',
	});
}

async function executePaymentType(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	return executeSimpleListResource(ctx, adapter, operation, itemIndex, {
		endpoint: '/payment-types',
		idParam: 'paymentTypeId',
		limitParam: 'paymentTypeLimit',
		offsetParam: 'paymentTypeOffset',
	});
}

async function executeSource(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	return executeSimpleListResource(ctx, adapter, operation, itemIndex, {
		endpoint: '/sources',
		idParam: 'sourceId',
		limitParam: 'sourceLimit',
		offsetParam: 'sourceOffset',
	});
}

async function executeCalendarTask(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	return executeSimpleListResource(ctx, adapter, operation, itemIndex, {
		endpoint: '/calendar-tasks',
		idParam: 'calendarTaskId',
		limitParam: 'calendarTaskLimit',
		offsetParam: 'calendarTaskOffset',
	});
}

async function executeWebhook(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const p = (n: string, f?: unknown) => ctx.getNodeParameter(n, itemIndex, f);
	switch (operation) {
		case 'getAll': {
			throw new NodeOperationError(
				ctx.getNode(),
				'ServiceFusion does not expose a supported webhook list endpoint for this node. Use Webhook → Create or Delete instead.',
			);
		}
		case 'create': {
			const d: Record<string, unknown> = {};
			if (p('webhookUrl')) d.url = p('webhookUrl');
			if (p('webhookEvents')) {
				try {
					d.events = JSON.parse(p('webhookEvents') as string);
				} catch {
					d.events = [p('webhookEvents')];
				}
			}
			if (p('webhookSecret')) d.secret = p('webhookSecret');
			const r = await adapter.createWebhook(d);
			return [{ json: r as unknown as IDataObject }];
		}
		case 'delete': {
			const id = p('webhookId') as string;
			await adapter.deleteWebhook(id);
			return [{ json: { success: true, webhookId: id } }];
		}
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown webhook operation: ${operation}`);
	}
}

async function executeOp(
	ctx: IExecuteFunctions,
	adapter: ServiceFusionAdapter,
	resource: string,
	operation: string,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	switch (resource) {
		case 'customer':
			return executeCustomer(ctx, adapter, operation, itemIndex);
		case 'job':
			return executeJob(ctx, adapter, operation, itemIndex);
		case 'estimate':
			return executeEstimate(ctx, adapter, operation, itemIndex);
		case 'invoice':
			return executeInvoice(ctx, adapter, operation, itemIndex);
		case 'technician':
			return executeTechnician(ctx, adapter, operation, itemIndex);
		case 'jobCategory':
			return executeJobCategory(ctx, adapter, operation, itemIndex);
		case 'jobStatus':
			return executeJobStatus(ctx, adapter, operation, itemIndex);
		case 'paymentType':
			return executePaymentType(ctx, adapter, operation, itemIndex);
		case 'source':
			return executeSource(ctx, adapter, operation, itemIndex);
		case 'calendarTask':
			return executeCalendarTask(ctx, adapter, operation, itemIndex);
		case 'webhook':
			return executeWebhook(ctx, adapter, operation, itemIndex);
		default:
			throw new NodeOperationError(ctx.getNode(), `Unknown resource: ${resource}`);
	}
}

export class ServiceFusion implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ServiceFusion',
		name: 'serviceFusion',
		icon: { light: 'file:servicefusion.svg', dark: 'file:servicefusion.dark.svg' },
		group: ['transform'],
		version: [1],
		description: 'Access ServiceFusion field service management API',
		subtitle: '={{$parameter["operation"]}}',
		defaults: { name: 'ServiceFusion' },
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		usableAsTool: true,
		credentials: [{ name: 'serviceFusionApi', required: true, testedBy: 'serviceFusionApi' }],
		properties: allProperties(),
	};

	methods = {
		credentialTest: {
			async serviceFusionApi(
				this: ICredentialTestFunctions,
				credential: ICredentialsDecrypted<ICredentialDataDecryptedObject>,
			): Promise<INodeCredentialTestResult> {
				const { clientId, clientSecret, baseUrl } = credential.data ?? {};
				const adapter = new ServiceFusionAdapter({
					clientId: clientId as string,
					clientSecret: clientSecret as string,
					baseUrl: (baseUrl as string) || 'https://api.servicefusion.com/v1',
				});
				try {
					await adapter.connect();
					await adapter.getCustomers({ limit: 1, sortBy: 'customer_name' });
					return { status: 'OK', message: 'Connected successfully' };
				} catch (error) {
					return { status: 'Error', message: (error as Error).message };
				} finally {
					await disconnectAdapter(adapter);
				}
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const credentials = await this.getCredentials('serviceFusionApi');
		let adapter: ServiceFusionAdapter | null = null;
		try {
			adapter = await createAdapter(credentials);
			const resource = this.getNodeParameter('resource', 0) as string;
			const operation = this.getNodeParameter('operation', 0) as string;
			for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
				try {
					const result = await executeOp(this, adapter, resource, operation, itemIndex);
					returnData.push(...result);
				} catch (error) {
					const apiError = getApiErrorDetails(error, adapter);
					if (this.continueOnFail()) {
						returnData.push({
							json: {
								error: (error as Error).message,
								...(apiError?.description ? { debug: apiError.description } : {}),
							},
							pairedItem: itemIndex,
						});
						continue;
					}
					if (apiError) {
						throw new NodeApiError(
							this.getNode(),
							{
								message: apiError.message,
								description: apiError.description,
							} as JsonObject,
							{
								itemIndex,
								message: apiError.message,
								description: apiError.description,
								httpCode: apiError.httpCode,
							},
						);
					}
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex });
				}
			}
		} catch (error) {
			const apiError = getApiErrorDetails(error, adapter);
			if (apiError) {
				throw new NodeApiError(
					this.getNode(),
					{
						message: apiError.message,
						description: apiError.description,
					} as JsonObject,
					{
						message: apiError.message,
						description: apiError.description,
						httpCode: apiError.httpCode,
					},
				);
			}
			throw new NodeOperationError(this.getNode(), error as Error);
		} finally {
			await disconnectAdapter(adapter);
		}
		return [returnData];
	}
}
