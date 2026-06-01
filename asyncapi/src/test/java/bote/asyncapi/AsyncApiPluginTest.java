package bote.asyncapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.build.MockManifest;
import software.amazon.smithy.build.PluginContext;
import software.amazon.smithy.build.SmithyBuildException;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.node.ObjectNode;

class AsyncApiPluginTest {

  private static final String MODEL =
      """
      $version: "2"
      namespace test
      use bote#kafkaJson
      use bote#kafkaTopic
      use bote#channel
      use bote#kafkaKey
      use bote#send

      @kafkaTopic(name: "a")
      structure ATopic {}

      @kafkaTopic(name: "b")
      structure BTopic {}

      @kafkaJson
      service A { operations: [PubA] }

      @kafkaJson
      service B { operations: [PubB] }

      @send
      @channel(ATopic)
      operation PubA { input: Payload }

      @send
      @channel(BTopic)
      operation PubB { input: Payload }

      structure Payload {
          @kafkaKey
          id: String
      }
      """;

  private Set<String> run(ObjectNode settings) {
    Model model = Model.assembler().discoverModels().addUnparsedModel("t.smithy", MODEL).assemble().unwrap();
    MockManifest manifest = new MockManifest();
    PluginContext context =
        PluginContext.builder().model(model).fileManifest(manifest).settings(settings).build();
    new AsyncApiPlugin().execute(context);
    return manifest.getFiles().stream()
        .map(Path::getFileName)
        .map(Path::toString)
        .collect(Collectors.toSet());
  }

  @Test
  void noSettingDocumentsEveryKafkaService() {
    assertEquals(Set.of("A.asyncapi.json", "B.asyncapi.json"), run(Node.objectNode()));
  }

  @Test
  void serviceSettingTargetsOneService() {
    Set<String> files = run(Node.objectNodeBuilder().withMember("service", "test#A").build());
    assertEquals(Set.of("A.asyncapi.json"), files);
  }

  @Test
  void unknownServiceSettingFails() {
    SmithyBuildException e =
        assertThrows(
            SmithyBuildException.class,
            () -> run(Node.objectNodeBuilder().withMember("service", "test#Nope").build()));
    assertTrue(e.getMessage().contains("test#Nope"));
  }
}
