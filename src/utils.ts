import { TextMapGetter, TextMapSetter } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import type { ServerInfo, Msg, MsgHdrs } from 'nats';

export function baseTraceAttrs(info: ServerInfo | undefined) {
  const attributes = {
    [SemanticAttributes.MESSAGING_SYSTEM]: 'nats',
    [SemanticAttributes.MESSAGING_PROTOCOL]: 'nats',
  };
  if (info) {
    attributes[SemanticAttributes.MESSAGING_PROTOCOL_VERSION] = info.version;
    attributes[SemanticAttributes.NET_PEER_NAME] = info.host;
    attributes[SemanticAttributes.NET_PEER_PORT] = `${info.port}`;
    if (info.client_ip) {
      attributes[SemanticAttributes.NET_PEER_IP] = info.client_ip;
    }
  }
  return attributes;
}

export function traceAttrs(info: ServerInfo | undefined, m: Msg) {
  const attributes = {
    ...baseTraceAttrs(info),
    [SemanticAttributes.MESSAGING_DESTINATION]: m.subject,
    [SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]: m.data
      ? m.data.length
      : 0,
  };

  if (m.reply) {
    attributes[SemanticAttributes.MESSAGING_CONVERSATION_ID] = m.reply;
  }

  return attributes;
}

export const natsContextGetter: TextMapGetter<MsgHdrs> = {
  keys(h: MsgHdrs) {
    if (h == null) return [];
    return (h as any).keys();
  },

  get(h: MsgHdrs, key: string) {
    if (h == null) return undefined;
    const res = h.get(key);
    return res === '' ? undefined : res;
  },
};

export const natsContextSetter: TextMapSetter<MsgHdrs> = {
  set(h: MsgHdrs, key: string, value: string) {
    if (h == null) return;
    h.set(key, value);
  },
};
