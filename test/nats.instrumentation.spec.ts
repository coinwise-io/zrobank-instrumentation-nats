import * as nats from 'nats';
import { context, trace } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { NatsInstrumentation } from '../src/nats.instrumentation';

const provider = new NodeTracerProvider();
const tracer = provider.getTracer('default');
const exporter = new InMemorySpanExporter();
const processor = new SimpleSpanProcessor(exporter);

provider.addSpanProcessor(processor);
const instrumentation = new NatsInstrumentation();

describe('NATS Instrumentation', () => {
  let connection: nats.NatsConnection;

  beforeAll(() => {
    instrumentation.enable();
  });

  afterAll(() => {
    instrumentation.disable();
  });

  beforeEach(async () => {
    exporter.reset();
    connection = await nats.connect({
      servers: 'localhost:4222',
    });
  });

  afterEach(async () => {
    await connection.drain();
  });

  it('should create and end a span for publish operations', async () => {
    const span = tracer.startSpan('test publish span');
    const ctx = trace.setSpan(context.active(), span);

    await context.with(ctx, async () => {
      connection.publish('test', Buffer.from('message'));
      span.end();
    });

    const spans = exporter.getFinishedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('test publish span');
  });

  it('should create and end a span for subscribe operations', async () => {
    const span = tracer.startSpan('test subscribe span');
    const ctx = trace.setSpan(context.active(), span);

    await context.with(ctx, async () => {
      const subscription = connection.subscribe('test');
      connection.publish('test', Buffer.from('message1'));

      for await (const msg of subscription) {
        expect(msg.data.toString()).toBe('message1');
        span.end();
        break;
      }
    });

    const spans = exporter.getFinishedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('test subscribe span');
  });
});
