import { InstrumentationConfig } from '@opentelemetry/instrumentation';
import { Span } from '@opentelemetry/api';
import { Msg as Message } from 'nats';

export type NatsInstrumentationConfig = InstrumentationConfig;

export interface NatsPublisherCustomAttributeFunction {
  (span: Span, subject: string, message: Message): void;
}
export interface NatsSubscriberCustomAttributeFunction {
  (span: Span, subject: string, message: Message): void;
}

export interface NatsCoreInstrumentationConfig extends InstrumentationConfig {
  /** hook for adding custom attributes before publish message is sent */
  publisherHook?: NatsPublisherCustomAttributeFunction;
  /** hook for adding custom attributes before subscribe message is processed */
  subscriberHook?: NatsSubscriberCustomAttributeFunction;
  /**
   * If passed, a span attribute will be added to all spans with key of the provided "moduleVersionAttributeName"
   * and value of the module version.
   */
  moduleVersionAttributeName?: string;
}
