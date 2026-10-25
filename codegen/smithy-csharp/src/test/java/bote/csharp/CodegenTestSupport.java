package bote.csharp;

import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpCodegenPlugin;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import software.amazon.smithy.build.MockManifest;
import software.amazon.smithy.build.PluginContext;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.node.ObjectNode;

final class CodegenTestSupport {

  private CodegenTestSupport() {}

  static Model model(String smithy) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", smithy)
        .assemble()
        .unwrap();
  }

  static Model modelFile(Path path) {
    return Model.assembler().discoverModels().addImport(path).assemble().unwrap();
  }

  // NSmithy formats the manifest's directory with csharpier when it is installed, so the manifest
  // gets a directory of its own rather than the default root.
  static MockManifest generate(Model model, String service, boolean server, boolean client) {
    MockManifest manifest;
    try {
      manifest = new MockManifest(Files.createTempDirectory("bote-csharp"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    ObjectNode settings =
        Node.objectNodeBuilder()
            .withMember("service", service)
            .withMember("baseNamespace", "")
            .withMember("generateServer", server)
            .withMember("generateClient", client)
            .build();
    new CSharpCodegenPlugin()
        .execute(
            PluginContext.builder().model(model).fileManifest(manifest).settings(settings).build());
    return manifest;
  }
}
