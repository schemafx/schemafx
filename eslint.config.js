import tsParser from '@typescript-eslint/parser';
import tsPlugin from '@typescript-eslint/eslint-plugin';
import prettierPlugin from 'eslint-plugin-prettier';
import prettier from 'prettier';
import path from 'node:path';

export default [
    {
        files: ['src/**/*.{ts,tsx}'],
        languageOptions: { parser: tsParser },
        plugins: {
            '@typescript-eslint': tsPlugin,
            prettier: prettierPlugin
        },
        rules: {
            ...tsPlugin.configs.recommended.rules,
            'prettier/prettier': [
                'error',
                await prettier.resolveConfig(path.resolve('./.prettierrc.json'))
            ],
            ...prettierPlugin.configs.recommended.rules
        }
    }
];
