import gitignore from 'eslint-config-flat-gitignore';
import tsParser from '@typescript-eslint/parser';
import tsPlugin from '@typescript-eslint/eslint-plugin';
import sonarjsPlugin from 'eslint-plugin-sonarjs';
import prettierPlugin from 'eslint-plugin-prettier';

export default [
    gitignore(),
    {
        files: ['{src,dev,tests}/**/*.{ts,tsx}'],
        languageOptions: {
            parser: tsParser,
            parserOptions: {
                project: './tsconfig.json'
            }
        },
        plugins: {
            '@typescript-eslint': tsPlugin,
            prettier: prettierPlugin,
            sonarjs: sonarjsPlugin
        },
        rules: {
            ...tsPlugin.configs.recommended.rules,
            ...tsPlugin.configs.strictTypeChecked,
            ...tsPlugin.configs.stylisticTypeChecked,
            'prettier/prettier': 'error',
            'arrow-body-style': ['error', 'as-needed'],
            '@typescript-eslint/no-floating-promises': 'error',
            '@typescript-eslint/no-misused-promises': 'error',
            '@typescript-eslint/return-await': 'error',
            '@typescript-eslint/restrict-template-expressions': 'error',
            '@typescript-eslint/no-shadow': 'error',
            'no-self-compare': 'error',
            '@typescript-eslint/consistent-type-imports': [
                'error',
                {
                    prefer: 'type-imports',
                    fixStyle: 'inline-type-imports'
                }
            ],
            '@typescript-eslint/no-import-type-side-effects': 'error',
            'sonarjs/prefer-immediate-return': 'error',
            ...prettierPlugin.configs.recommended.rules
        }
    }
];
