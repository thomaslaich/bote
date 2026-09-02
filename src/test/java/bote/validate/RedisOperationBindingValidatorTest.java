package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class RedisOperationBindingValidatorTest {

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

  @Test
  void pubSubPublishWithReplyOutputIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisPubSubJson
        use bote#redisPublish
        use bote#command
        use bote#reply

        @redisPubSubJson
        service Service { operations: [Lookup] }

        @redisPublish(channel: "lookups")
        operation Lookup { input: Request, output: Response }

        @command
        structure Request { id: String }

        @reply
        structure Response { value: String }
        """;
    assertTrue(
        validate(model).stream()
            .anyMatch(e -> e.getMessage().contains("must not define an output")));
  }

  @Test
  void streamAddWithReplyOutputIsValid() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStreamsJson
        use bote#redisStreamAdd
        use bote#command
        use bote#reply

        @redisStreamsJson
        service Service { operations: [Lookup] }

        @redisStreamAdd(stream: "lookups")
        operation Lookup { input: Request, output: Response }

        @command
        structure Request { id: String }

        @reply
        structure Response { value: String }
        """;
    assertTrue(validate(model).isEmpty());
  }

  @Test
  void streamOutputWithoutReplyTraitIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStreamsJson
        use bote#redisStreamAdd
        use bote#command

        @redisStreamsJson
        service Service { operations: [Lookup] }

        @redisStreamAdd(stream: "lookups")
        operation Lookup { input: Request, output: Response }

        @command
        structure Request { id: String }

        structure Response { value: String }
        """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getMessage().contains("bote#reply")));
  }
}
