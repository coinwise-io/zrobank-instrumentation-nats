/* eslint-disable @typescript-eslint/no-this-alias */
/* eslint-disable @typescript-eslint/no-unused-vars */
import type * as Nats from 'nats';
import { MsgHdrs, MsgHdrsImpl } from 'nats';
import {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition,
  isWrapped,
} from '@opentelemetry/instrumentation';
import {
  diag,
  propagation,
  trace,
  context,
  SpanKind,
  SpanStatusCode,
  Context,
  Span,
  ROOT_CONTEXT,
} from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import * as utils from './utils';
import { NatsInstrumentationConfig } from './types';

const NATS: string = 'nats';
const CONNECT: string = 'connect';
const PUBLISH: string = 'publish';
const REQUEST: string = 'request';
const SUBSCRIBE: string = 'subscribe';
const VERSION: string = '2.28.0';
const MODULE_NAME: string = 'zrobank-instrumentation-nats';
const TEMP_DESTINATION: string = '(temporary_destination)';

interface NatsHelpers {
  subjects: Set<string>;
  headers?: () => MsgHdrs;
}

export class NatsInstrumentation extends InstrumentationBase {
  protected _natsHelpers: NatsHelpers;

  constructor(
    protected override _config: NatsInstrumentationConfig = { enabled: true },
  ) {
    super(MODULE_NAME, VERSION, _config);
    this._natsHelpers = {
      subjects: new Set(),
      headers: () => new MsgHdrsImpl(1, 'natsHelperHeader - MsgHdrsImpl'),
    };
  }

  // Initializes the instrumentation by patching the NATS module to add telemetry
  protected init(): InstrumentationNodeModuleDefinition {
    diag.debug(
      JSON.stringify({
        message: 'Initialization of NATS Instrumentation',
        function: 'init',
      }),
    );
    // Defines the module to be instrumented and the methods to wrap
    return new InstrumentationNodeModuleDefinition(
      NATS,
      [VERSION],
      (moduleExports, moduleVersion) => {
        diag.debug(
          JSON.stringify({
            message: `Applying nats patch for nats@${moduleVersion}`,
            module: 'NATS',
            function: 'init',
            version: moduleVersion,
          }),
        );
        const { headers } = moduleExports;
        this._natsHelpers.headers = headers;
        // Wrapping the connect function to enable tracing when a connection is established
        this.ensureWrapped(
          moduleVersion,
          moduleExports,
          CONNECT,
          this.wrapConnect.bind(this),
        );
        return moduleExports;
      },
      (moduleExports, moduleVersion) => {
        if (moduleExports === undefined) return;
        // Clean up when unpatching the module
        this._unwrap(moduleExports, CONNECT);
        diag.debug(
          JSON.stringify({
            message: `Removing nats patch for nats@${moduleVersion}`,
            module: 'NATS',
            function: 'init',
            version: moduleVersion,
          }),
        );
      },
    );
  }

  // Wraps the NATS connect method to intercept the connection and add proxies to specific NATS functions
  private wrapConnect(originalFunc: typeof import('nats').connect) {
    const instrumentation = this;
    // Proxy traps to wrap specific methods for telemetry
    const traps = {
      get: function get(target: Nats.NatsConnection, prop: string) {
        // Intercepts method calls to add telemetry
        switch (prop) {
          case SUBSCRIBE:
            return instrumentation.wrapSubscribe(target.subscribe);
          case PUBLISH:
            return instrumentation.wrapPublish(target.publish);
          case REQUEST:
            return instrumentation.wrapRequest(target.request);
          default:
            return (target as any)[prop];
        }
      },
    };
    // Returns a modified connect function that adds the above proxies
    return async function connect(
      opts?: Nats.ConnectionOptions | undefined,
    ): Promise<Nats.NatsConnection> {
      // Establishes the original connection
      const nc = await originalFunc(opts);
      // Returns a proxied connection for telemetry-enhanced interactions
      return new Proxy(nc, traps);
    };
  }

  private wrapSubscribe(originalFunc: Nats.NatsConnection['subscribe']) {
    const instrumentation = this;
    return function subscribe(
      this: Nats.NatsConnection,
      subject: string,
      opts?: Nats.SubscriptionOptions,
    ): Nats.Subscription {
      const nc = this;
      // Function to generate a context and span from a NATS message
      const genSpanAndContextFromMessage = (m: Nats.Msg): [Context, Span] => {
        // Extracts context from incoming message headers for proper trace propagation
        const parentContext = propagation.extract(
          ROOT_CONTEXT,
          m.headers,
          utils.natsContextGetter,
        );
        // Starts a new span with appropriate attributes and links it to the parent context
        const span = instrumentation.tracer.startSpan(
          `${m.subject} process`,
          {
            attributes: {
              ...utils.traceAttrs(nc.info, m),
              [SemanticAttributes.MESSAGING_OPERATION]: 'process',
              [SemanticAttributes.MESSAGING_DESTINATION_KIND]: 'subject',
            },
            kind: m.reply ? SpanKind.SERVER : SpanKind.CONSUMER,
          },
          parentContext,
        );
        // Links the new span to the parent context for downstream operations
        const ctx = trace.setSpan(parentContext, span);
        return [ctx, span];
      };

      // Wraps the user-provided callback to incorporate tracing
      if (opts?.callback) {
        const originalCallBack = opts.callback;
        opts.callback = function wrappedCallback(
          err: Nats.NatsError | null,
          msg: Nats.Msg,
        ) {
          if (err) {
            originalCallBack(err, msg);
            return;
          }
          // Setup message for tracing before processing in callback
          msg = instrumentation.setupMessage(msg, nc);
          const [ctx, span] = genSpanAndContextFromMessage(msg);
          try {
            context.with(ctx, originalCallBack, undefined, err, msg);
            span.setStatus({
              code: SpanStatusCode.OK,
            });
          } catch (err) {
            // Updates span status on error and records the exception
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: err.message,
            });
            span.recordException(err);
            throw err;
          } finally {
            instrumentation.cleanupMessage(msg);
            span.end();
          }
        };
      }

      // Calls the original subscribe function and returns the subscription
      const sub = originalFunc.apply(this, [subject, opts]);
      if (opts?.callback) {
        // Returns the original subscription if a callback is used (not wrapping async iterator)
        return sub;
      }

      // Wraps the subscription iterator to include tracing
      const wrappedIterator = (async function* wrappedIterator() {
        for await (let m of sub) {
          m = instrumentation.setupMessage(m, nc);
          const [_ctx, span] = genSpanAndContextFromMessage(m);

          try {
            yield m;
            span.setStatus({
              code: SpanStatusCode.OK,
            });
          } catch (err) {
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: err.message,
            });
            span.recordException(err);
            throw err;
          } finally {
            instrumentation.cleanupMessage(m);
            span.end();
          }
        }
      })();

      // Ensures the wrapped iterator maintains the original subscription's properties
      Object.assign(wrappedIterator, sub);
      return wrappedIterator as any;
    };
  }

  private wrapPublish(originalFunc: Nats.NatsConnection['publish']) {
    const instrumentation = this;

    return function publish(
      this: Nats.NatsConnection,
      subject: string,
      data: Uint8Array,
      options?: Nats.PublishOptions,
    ): void {
      const nc = this;
      // Determines if the destination is temporary for proper attribute setting
      const isTemporaryDestination =
        instrumentation.isTemporaryDestination(subject);
      const destination = isTemporaryDestination ? TEMP_DESTINATION : subject;
      // Starts a span for the publish operation with relevant attributes
      const span = instrumentation.tracer.startSpan(`${destination} send`, {
        attributes: {
          ...utils.baseTraceAttrs(nc.info),
          [SemanticAttributes.MESSAGING_DESTINATION_KIND]: 'subject',
          [SemanticAttributes.MESSAGING_DESTINATION]: destination,
          [SemanticAttributes.MESSAGING_TEMP_DESTINATION]:
            isTemporaryDestination,
          [SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]: data
            ? data.length
            : 0,
        },
        kind: SpanKind.PRODUCER,
      });

      // Sets specific attributes based on the message's nature and response settings
      if (isTemporaryDestination) {
        span.setAttribute(
          SemanticAttributes.MESSAGING_CONVERSATION_ID,
          subject,
        );
      } else if (options?.reply) {
        span.setAttribute(
          SemanticAttributes.MESSAGING_CONVERSATION_ID,
          options.reply,
        );
      }
      // Sets the current active span for context management
      const ctx = trace.setSpan(context.active(), span);
      // Injects tracing context into message headers for downstream propagation
      const h: MsgHdrs = options?.headers
        ? options.headers
        : instrumentation._natsHelpers.headers!();
      propagation.inject(ctx, h, utils.natsContextSetter);

      try {
        // Executes the original publish function within the traced context
        context.with(ctx, originalFunc, this, subject, data, {
          ...options,
          headers: h,
        });
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        // Records exceptions and updates span status on error
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
        throw err;
      } finally {
        // Ends the span once the publish operation is complete
        span.end();
      }
    };
  }

  private wrapRespond(
    originalFunc: Nats.Msg['respond'],
    nc: Nats.NatsConnection,
  ) {
    const instrumentation = this;
    // Wraps the 'respond' method of a NATS message to include tracing
    return function respond(
      this: Nats.Msg,
      data?: Uint8Array | undefined,
      options?: Nats.PublishOptions | undefined,
    ) {
      const msg = this;
      // Determines if the response is to a temporary destination or a standard reply address
      const destination = msg.reply || TEMP_DESTINATION;
      // Starts a new tracing span with relevant information about the message being responded to
      const span = instrumentation.tracer.startSpan(`${destination} send`, {
        attributes: {
          ...utils.baseTraceAttrs(nc.info),
          [SemanticAttributes.MESSAGING_DESTINATION_KIND]: 'subject',
          [SemanticAttributes.MESSAGING_DESTINATION]: destination,
          [SemanticAttributes.MESSAGING_TEMP_DESTINATION]: true,
          [SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]: data
            ? data.length
            : 0,
          [SemanticAttributes.MESSAGING_CONVERSATION_ID]: msg.reply, // Uses the reply-to subject as the conversation ID if available
        },
        kind: SpanKind.PRODUCER, // Classifies this operation as a 'PRODUCER' because it sends a response
      });

      // Attaches the newly created span to the current context
      const ctx = trace.setSpan(context.active(), span);
      // Prepares headers for the response, injecting trace context for continued propagation
      const h: MsgHdrs = msg.headers
        ? msg.headers
        : instrumentation._natsHelpers.headers!();
      propagation.inject(ctx, h, utils.natsContextSetter);

      try {
        // Executes the original respond function within the traced context, ensuring the tracing data is propagated
        context.with(ctx, originalFunc, this, data, {
          ...options,
          headers: h,
        });
        // Marks the span as successful upon successful execution of the respond function
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
      } finally {
        span.end();
      }
    };
  }

  private wrapRequest(originalFunc: Nats.NatsConnection['request']) {
    const instrumentation = this;
    // This function wraps the NATS connection's 'request' method to include distributed tracing
    return async function request(
      this: Nats.NatsConnection,
      subject: string,
      data?: Uint8Array,
      opts?: Nats.RequestOptions,
    ) {
      const nc = this;
      // Starts a new span for the request operation, attaching base trace attributes and specifying the span as a 'CLIENT' operation
      const span = instrumentation.tracer.startSpan(`${subject} request`, {
        attributes: {
          ...utils.baseTraceAttrs(nc.info), // Utility function to gather base attributes related to the NATS connection
        },
        kind: SpanKind.CLIENT,
      });

      try {
        // Executes the original request method within a context that includes the new span, allowing downstream tracing
        const res = await context.with(
          trace.setSpan(context.active(), span), // Sets the current span as active in the tracing context
          originalFunc, // The original NATS 'request' function to be executed
          this, // Contextual 'this' refers to the NATS connection
          subject,
          data, // The message payload as a Uint8Array
          opts,
        );
        // Marks the span as successful if the request completes without throwing an error
        span.setStatus({ code: SpanStatusCode.OK });
        return res;
      } catch (err) {
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
        throw err;
      } finally {
        span.end();
      }
    };
  }

  private isTemporaryDestination(subject: string) {
    // Checks if the given subject is considered a temporary destination.
    // Temporary destinations are typically used for responses to requests and are not intended to persist.
    // This function uses a set (_natsHelpers.subjects) to track subjects that have been marked as temporary.
    return this._natsHelpers.subjects.has(subject);
  }

  private setupMessage(msg: Nats.Msg, nc: Nats.NatsConnection): Nats.Msg {
    const instrumentation = this;
    // Adds the message's subject to a set of known subjects, treating it as potentially temporary.
    if (msg) {
      this._natsHelpers.subjects.add(msg.subject);
    }

    // Defines property traps for the message proxy to intercept and handle specific properties specially.
    const traps = {
      get: function get(target: Nats.Msg, prop: string) {
        if (prop === 'respond') {
          return instrumentation.wrapRespond(target.respond, nc);
        }
        return (target as any)[prop];
      },
    };

    return new Proxy(msg, traps);
  }

  private cleanupMessage(msg: Nats.Msg) {
    // Cleans up any temporary settings for a message after it has been processed.
    if (msg.reply) {
      // If the message has a reply property, it indicates a temporary usage scenario;
      // hence, we remove it from the set of tracked temporary subjects.
      this._natsHelpers.subjects.delete(msg.subject);
    }
  }

  public ensureWrapped(
    moduleVersion: string | undefined,
    obj: any,
    methodName: string,
    wrapper: (original: any) => any,
  ) {
    diag.debug(
      JSON.stringify({
        message: `Applying ${methodName} patch for nats@${moduleVersion}`,
        module: 'NATS',
        function: 'ensureWrapped',
        method: methodName,
        version: moduleVersion,
      }),
    );
    // Checks if the method has already been wrapped to prevent double-wrapping which could lead to issues.
    if (isWrapped(obj[methodName])) {
      this._unwrap(obj, methodName);
    }
    // Wraps the method with the provided wrapper function to inject additional behavior,
    // such as to add tracing capabilities to NATS operations.
    this._wrap(obj, methodName, wrapper);
  }
}
