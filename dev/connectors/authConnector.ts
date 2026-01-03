import { Connector } from '../../src/index';

export type AuthConnectorOptions = {
    name: string;
    id?: string;
    devEmail?: string;
    serverUri: string;
};

export default class AuthConnector extends Connector {
    serverUri: string;
    devEmail: string;

    constructor(options: AuthConnectorOptions) {
        super(options);

        this.serverUri = options.serverUri;
        this.devEmail = options.devEmail || 'dev@schemafx.com';
    }

    override async getAuthUrl(): Promise<string> {
        return new URL(`/api/connectors/${this.id}/auth/callback`, this.serverUri).href;
    }

    override async authorize() {
        return {
            name: this.devEmail,
            content: this.devEmail,
            email: this.devEmail
        };
    }

    override async listTables() {
        return [];
    }

    override async getTable() {
        return undefined;
    }
}
