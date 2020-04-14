package com.netflix.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class KSGatewayListener {
  private static final Logger LOG = LoggerFactory.getLogger(KSGatewayListener.class);
  private static final String KS_HOST_PROP = "keystone.gateway.host";
  private static final String KS_PORT_PROP = "keystone.gateway.port";
  private static final String KS_SCAN_TOPIC = "iceberg_scans";

  private interface Writer {
    void write(JsonGenerator gen) throws IOException;
  }

  public static void initialize(String appType, String appId, Configuration conf) {
    try {
      String remoteHost = conf.get(KS_HOST_PROP);
      if (remoteHost != null) {
        int remotePort = conf.getInt(KS_PORT_PROP, 80);
        LOG.info("Sending Iceberg events to {}:{}", remoteHost, remotePort);

        KSGatewayListener listener = new KSGatewayListener(appType,
            appId,
            resolve(conf, "genie.job.id"),
            remoteHost,
            remotePort);

        Listeners.register(listener::scan, ScanEvent.class);
      }

    } catch (Exception e) {
      LOG.warn("Failed to initialize KSGatewayListener", e);
    }
  }

  private static String resolve(Configuration conf, String... properties) {
    for (String p : properties) {
      String val;
      if (p.startsWith("env.")) {
        val = System.getenv(p.replaceFirst("env\\.", ""));
      } else if(p.startsWith("sys.")) {
        val = System.getProperty(p.replaceFirst("sys\\.", ""));
      } else {
        val = conf.get(p, null);
      }

      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private static final String hostname = getHostname();

  private static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to determine hostname", e);
    }
    return null;
  }

  private final ThreadLocal<HttpClient> client = ThreadLocal.withInitial(DefaultHttpClient::new);
  private final String remoteHost;
  private final int remotePort;
  private final String appType;
  private final Writer commonFieldsWriter;

  private KSGatewayListener(String appType, String appId, String genieId, String remoteHost, int remotePort) {
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
    this.appType = appType;
    this.commonFieldsWriter = gen -> {
      gen.writeStringField("app_type", appType);
      if (appId != null) {
        gen.writeStringField("app_id", appId);
      }
      gen.writeStringField("genie_id", genieId);
    };
  }

  private void send(String topic, String json) {
    HttpPost request = new HttpPost("/REST/v1/stream/" + topic);
    request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
    try {
      HttpResponse response = client.get().execute(new HttpHost(remoteHost, remotePort), request);
      int status = response.getStatusLine().getStatusCode();
      switch (status) {
        case 200:
          break;
        default:
          LOG.warn("Error sending: " + status + ": " + response.getStatusLine().getReasonPhrase() +
              " - " + EntityUtils.toString(response.getEntity()));
          LOG.warn("Message payload: " + json);
      }

      // consume the response so the client can be reused
      EntityUtils.consumeQuietly(response.getEntity());

    } catch (IOException e) {
      // discard the failed client
      client.remove();

      LOG.warn(
          String.format("Failed to send metrics to %s:%d/REST/v1/stream/%s",
              remoteHost, remotePort, topic),
          e);
      LOG.warn("Message payload: " + json);
    }
  }

  private <T> String toJson(T event, Writer writeFunc) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();

      generator.writeStartObject(); // start message
      generator.writeStringField("appName", appType);
      generator.writeStringField("hostname", hostname);
      generator.writeBooleanField("ack", false);

      generator.writeObjectFieldStart("event");
      generator.writeStringField("uuid", UUID.randomUUID().toString());

      generator.writeObjectFieldStart("payload");
      commonFieldsWriter.write(generator);
      writeFunc.write(generator);
      generator.writeEndObject(); // end payload

      generator.writeEndObject(); // end event

      generator.writeEndObject(); // end message

      generator.flush();

      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", event);
    }
  }

  private void scan(ScanEvent scan) {
    send(KS_SCAN_TOPIC, toJson(scan, gen -> {
      gen.writeStringField("table", scan.tableName());
      gen.writeStringField("filter", scan.filter().toString());
      gen.writeStringField("projection", scan.projection().asStruct().toString());
      gen.writeStringField("snapshot_id", Long.toString(scan.snapshotId()));
    }));
  }
}
