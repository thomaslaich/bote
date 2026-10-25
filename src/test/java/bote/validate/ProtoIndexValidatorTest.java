package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class ProtoIndexValidatorTest {

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

  @Test
  void fullyIndexedPayloadsAreValid() {
    String model =
        """
        $version: "2"
        namespace test
        use alloy.proto#protoIndex
        use bote#channels
        use bote#event
        use bote#kafkaHeader
        use bote#kafkaProtobuf
        use bote#kafkaTopic

        @kafkaProtobuf
        @channels([Alerts])
        service Telemetry { operations: [Record] }

        @kafkaTopic(name: "readings")
        operation Record { input: Reading }

        @input
        structure Reading {
            @protoIndex(1)
            sensorId: String

            @protoIndex(2)
            value: Integer

            /// Travels only as a Kafka header; the index is reserved.
            @protoIndex(3)
            @kafkaHeader(name: "x-trace-id")
            traceId: String
        }

        @event
        structure Alert {
            @protoIndex(1)
            sensorId: String
        }

        @kafkaTopic(name: "alerts")
        union Alerts {
            @protoIndex(1)
            alert: Alert
        }
        """;
    List<ValidationEvent> events = validate(model);
    assertTrue(events.isEmpty(), events::toString);
  }

  @Test
  void missingProtoIndexIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use alloy.proto#protoIndex
        use bote#kafkaProtobuf
        use bote#kafkaTopic

        @kafkaProtobuf
        service Telemetry { operations: [Record] }

        @kafkaTopic(name: "readings")
        operation Record { input: Reading }

        @input
        structure Reading {
            @protoIndex(1)
            sensorId: String

            value: Integer
        }
        """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getMessage().contains("protoIndex")));
  }

  @Test
  void missingProtoIndexOnChannelMemberIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use alloy.proto#protoIndex
        use bote#channels
        use bote#event
        use bote#kafkaProtobuf
        use bote#kafkaTopic

        @kafkaProtobuf
        @channels([Alerts])
        service Telemetry {}

        @kafkaTopic(name: "alerts")
        union Alerts { alert: Alert }

        @event
        structure Alert {
            @protoIndex(1)
            sensorId: String
        }
        """;
    assertTrue(
        validate(model).stream().anyMatch(e -> e.getMessage().contains("test#Alerts$alert")));
  }

  @Test
  void jsonProtocolPayloadsNeedNoProtoIndex() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaTopic

        @kafkaJson
        service Orders { operations: [Submit] }

        @kafkaTopic(name: "orders")
        operation Submit { input: SubmitOrder }

        @input
        structure SubmitOrder { orderId: String }
        """;
    assertTrue(validate(model).isEmpty());
  }
}
