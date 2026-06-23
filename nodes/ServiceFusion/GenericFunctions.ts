import type { ICredentialDataDecryptedObject } from 'n8n-workflow';
import { ServiceFusionAdapter } from './vendor/servicefusion-adapter.bundle';

/**
 * Create a configured ServiceFusionAdapter from n8n credential data.
 */
export async function createAdapter(
	credentials: ICredentialDataDecryptedObject,
): Promise<ServiceFusionAdapter> {
	const adapter = new ServiceFusionAdapter({
		clientId: credentials.clientId as string,
		clientSecret: credentials.clientSecret as string,
		baseUrl: (credentials.baseUrl as string) || 'https://api.servicefusion.com/v1',
	});

	await adapter.connect();
	return adapter;
}

/**
 * Safely disconnect the adapter in a finally block.
 */
export async function disconnectAdapter(adapter: ServiceFusionAdapter | null) {
	if (adapter) {
		try {
			await adapter.disconnect();
		} catch {
			// Ignore disconnect errors - adapter cleanup only
		}
	}
}
