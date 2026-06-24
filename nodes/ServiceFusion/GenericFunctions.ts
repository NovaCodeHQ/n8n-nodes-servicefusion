import type { ICredentialDataDecryptedObject } from 'n8n-workflow';
import { ServiceFusionAdapter } from './vendor/servicefusion-adapter.bundle';

export interface AdapterDebugState {
	lastRequest?: {
		url?: string;
		method?: string;
	};
	lastResponse?: {
		url?: string;
		status?: number;
	};
	lastAuthError?: unknown;
}

type DebuggableServiceFusionAdapter = ServiceFusionAdapter & {
	__n8nDebug?: AdapterDebugState;
};

function attachAdapterDebug(adapter: DebuggableServiceFusionAdapter) {
	const debug: AdapterDebugState = {};
	const eventedAdapter = adapter as DebuggableServiceFusionAdapter & {
		on: (event: string, listener: (payload: unknown) => void) => void;
	};
	adapter.__n8nDebug = debug;
	eventedAdapter.on('request', (request: unknown) => {
		debug.lastRequest = request as AdapterDebugState['lastRequest'];
	});
	eventedAdapter.on('response', (response: unknown) => {
		debug.lastResponse = response as AdapterDebugState['lastResponse'];
	});
	eventedAdapter.on('auth-error', (error: unknown) => {
		debug.lastAuthError = error;
	});
}

export function getAdapterDebugState(adapter: ServiceFusionAdapter | null | undefined) {
	return (adapter as DebuggableServiceFusionAdapter | null | undefined)?.__n8nDebug;
}

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

	attachAdapterDebug(adapter as DebuggableServiceFusionAdapter);
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
