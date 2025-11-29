import fastify from 'fastify';
import 'dotenv/config';

const app = fastify({
    logger: true
});

// Start
try {
    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;

    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
