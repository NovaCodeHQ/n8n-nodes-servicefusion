import { configWithoutCloudSupport } from '@n8n/node-cli/eslint';
import { globalIgnores } from 'eslint/config';

export default [globalIgnores(['nodes/**/vendor/**/*']), ...configWithoutCloudSupport];
