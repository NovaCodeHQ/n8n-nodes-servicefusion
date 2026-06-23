type ServiceFusionRecord = Record<string, unknown>;
type ServiceFusionList = ServiceFusionRecord[];

export interface ServiceFusionConfig {
	clientId: string;
	clientSecret: string;
	baseUrl?: string;
	options?: {
		rateLimit?: number;
		retryAttempts?: number;
		retryDelay?: number;
		timeout?: number;
		autoRefreshToken?: boolean;
		maxConcurrent?: number;
	};
}

export interface BatchSyncOptions {
	matchBy: 'externalId' | 'customerId' | 'description';
	updateExisting: boolean;
	createNew: boolean;
}

export class ServiceFusionAdapter {
	constructor(config: ServiceFusionConfig);
	connect(): Promise<void>;
	disconnect(): Promise<void>;
	isConnected(): boolean;
	getCustomers(options?: ServiceFusionRecord): Promise<ServiceFusionList>;
	getCustomer(id: string): Promise<ServiceFusionRecord>;
	createCustomer(data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	updateCustomer(id: string, data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	deleteCustomer(id: string): Promise<void>;
	searchCustomers(filters: ServiceFusionRecord): Promise<ServiceFusionList>;
	getJobs(filters?: ServiceFusionRecord): Promise<ServiceFusionList>;
	getJob(id: string): Promise<ServiceFusionRecord>;
	createJob(data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	updateJob(id: string, data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	deleteJob(id: string): Promise<void>;
	searchJobs(filters: ServiceFusionRecord): Promise<ServiceFusionList>;
	getAllJobs(filters?: ServiceFusionRecord): Promise<ServiceFusionList>;
	batchSyncJobs(jobs: unknown[], options: BatchSyncOptions): Promise<ServiceFusionRecord>;
	getEstimates(jobId?: string): Promise<ServiceFusionList>;
	getEstimate(id: string): Promise<ServiceFusionRecord>;
	createEstimate(data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	updateEstimate(id: string, data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	convertEstimateToJob(id: string): Promise<ServiceFusionRecord>;
	getInvoices(filters?: ServiceFusionRecord): Promise<ServiceFusionList>;
	getInvoice(id: string): Promise<ServiceFusionRecord>;
	createInvoice(data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	updateInvoice(id: string, data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	sendInvoice(id: string, email: string): Promise<void>;
	getTechnicians(options?: ServiceFusionRecord): Promise<ServiceFusionList>;
	getTechnician(id: string): Promise<ServiceFusionRecord>;
	getSchedule(technicianId: string, date: Date): Promise<ServiceFusionRecord>;
	assignJob(jobId: string, technicianId: string): Promise<ServiceFusionRecord>;
	createWebhook(data: ServiceFusionRecord): Promise<ServiceFusionRecord>;
	getWebhooks(): Promise<ServiceFusionList>;
	deleteWebhook(id: string): Promise<void>;
}
