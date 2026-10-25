package bote.csharp;

import bote.BoteIndex.Address;
import bote.csharp.Contract.Channel;
import bote.csharp.Contract.Command;
import bote.csharp.Contract.Event;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpNaming;
import io.github.thomaslaich.nsmithy.csharp.codegen.GenerationContext;
import io.github.thomaslaich.nsmithy.csharp.codegen.generators.SchemaGenerator;
import io.github.thomaslaich.nsmithy.csharp.codegen.writer.CSharpWriter;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import software.amazon.smithy.model.shapes.Shape;

/**
 * Generates a bote service's C# messaging surface. The owner side handles commands and publishes
 * events; the client side sends commands and handles events. Each side goes to its own file, and
 * the codecs both sides share go to a third.
 *
 * <p>Subclasses supply the broker: how payloads are encoded, which inbounds and outbounds carry
 * them, and how consumers are registered.
 */
abstract class MessagingGenerator {

  static final String TASK = "System.Threading.Tasks.Task";
  static final String CANCELLATION_TOKEN = "System.Threading.CancellationToken";

  protected final GenerationContext context;
  protected final Contract contract;
  protected final String service;

  MessagingGenerator(GenerationContext context, Contract contract) {
    this.context = context;
    this.contract = contract;
    this.service = contract.serviceName();
  }

  /** The runtime namespace of the broker, such as {@code Bote.Kafka}. */
  abstract String runtimeNamespace();

  /** Writes the codec fields and the encode and decode methods both sides use. */
  abstract void writeCodecs(CSharpWriter writer);

  /** The registration of a hosted consumer for {@code inbounds}, as an expression. */
  abstract String consumerRegistration(List<String> inbounds);

  /** The parameters a consumer registration method takes, after the service collection. */
  abstract String consumerParameters();

  /** The runtime sender type that the generated registrations hand to publishers and clients. */
  abstract String senderType();

  /** Extra arguments of an outbound to {@code address}, after the encoders; empty by default. */
  String outboundOptions(Address address) {
    return "";
  }

  /** Writes all files for the service. */
  void run() {
    String namespace =
        context.settings().csharpNamespace(contract.service().getId().getNamespace());
    String directory = namespace.replace('.', '/') + "/" + service;
    boolean server = context.settings().generateServer();
    boolean client = context.settings().generateClient();
    if (!server && !client) {
      return;
    }
    write(directory + ".Messaging.g.cs", namespace, this::writeShared);
    if (server) {
      write(directory + ".Messaging.Server.g.cs", namespace, this::writeOwner);
    }
    if (client) {
      write(directory + ".Messaging.Client.g.cs", namespace, this::writeClient);
    }
  }

  void write(String path, String namespace, Consumer<CSharpWriter> body) {
    context
        .writerDelegator()
        .useFileWriter(
            path,
            namespace,
            writer -> {
              writer.addImport("Bote");
              writer.addImport(runtimeNamespace());
              body.accept(writer);
            });
  }

  private void writeShared(CSharpWriter writer) {
    writer.addImport("NSmithy.Core.Serde");
    writer.addImport("NSmithy.Codecs.Json");
    writer.write("internal static partial class $LMessaging", service);
    writer.openBlock("{");
    writeCodecs(writer);
    writer.closeBlock("}");
  }

  // Owner: handles commands and publishes events.

  private void writeOwner(CSharpWriter writer) {
    for (Command command : contract.commands()) {
      writeCommandHandler(writer, command);
    }
    for (Channel channel : contract.channels()) {
      writePublisher(writer, channel);
    }

    writer.write("internal static partial class $LMessaging", service);
    writer.openBlock("{");
    for (Command command : contract.commands()) {
      writer.write(
          "internal static readonly InboundBinding $LInbound = $L;",
          command.name(),
          commandInbound(command));
    }
    for (Channel channel : contract.channels()) {
      for (Event event : channel.events()) {
        writer.write(
            "internal static readonly OutboundBinding<$L> $L$LOutbound = new($L, Encode$L$L$L);",
            typeName(event.shape()),
            channel.name(),
            event.name(),
            facts(channel),
            channel.name(),
            event.name(),
            outboundOptions(channel.address()));
      }
    }
    writer.closeBlock("}");

    writeExtensions(
        writer,
        w -> {
          if (!contract.commands().isEmpty()) {
            writeConsumerRegistration(
                w,
                service + "CommandConsumer",
                contract.commands().stream()
                    .map(c -> service + "Messaging." + c.name() + "Inbound")
                    .toList());
          }
          for (Channel channel : contract.channels()) {
            writeSingleton(w, channel.name() + "Publisher", "sender");
          }
        });
  }

  private void writeCommandHandler(CSharpWriter writer, Command command) {
    writer.write(
        """
        public interface I$LHandler
        {
            $L HandleAsync($L command, $L cancellationToken = default);
        }
        """,
        command.name(),
        replyTask(command),
        typeName(command.input()),
        CANCELLATION_TOKEN);
  }

  private void writePublisher(CSharpWriter writer, Channel channel) {
    String signatures =
        channel.events().stream()
            .map(
                event ->
                    String.format(
                        "%s Publish%sAsync(%s @event, %s cancellationToken = default);",
                        TASK, event.name(), typeName(event.shape()), CANCELLATION_TOKEN))
            .collect(Collectors.joining("\n"));
    String methods =
        channel.events().stream()
            .map(
                event ->
                    String.format(
                        """
                        public %s Publish%sAsync(%s @event, %s cancellationToken = default) =>
                            _sender.SendAsync(%sMessaging.%s%sOutbound, @event, cancellationToken);
                        """,
                        TASK,
                        event.name(),
                        typeName(event.shape()),
                        CANCELLATION_TOKEN,
                        service,
                        channel.name(),
                        event.name()))
            .collect(Collectors.joining("\n"));
    writer.write(
        """
        public interface I$1LPublisher
        {
            ${2L|}
        }

        public sealed class $1LPublisher(IMessageSender sender) : I$1LPublisher
        {
            private readonly IMessageSender _sender =
                sender ?? throw new System.ArgumentNullException(nameof(sender));

            ${3L|}
        }
        """,
        channel.name(),
        signatures,
        methods);
  }

  // Client: sends commands and handles events.

  private void writeClient(CSharpWriter writer) {
    if (!contract.commands().isEmpty()) {
      writeCommandClient(writer);
    }
    for (Channel channel : contract.channels()) {
      writeChannelHandler(writer, channel);
    }

    writer.write("internal static partial class $LMessaging", service);
    writer.openBlock("{");
    for (Command command : contract.commands()) {
      writer.write(
          "internal static readonly $L $LOutbound = new($L, Encode$L$L$L);",
          outboundType(command),
          command.name(),
          facts(command),
          command.name(),
          command.output().map(o -> ", Decode" + command.name() + "Reply").orElse(""),
          outboundOptions(command.address()));
    }
    for (Channel channel : contract.channels()) {
      writer.write(
          """
          internal static readonly InboundBinding $2LInbound = InboundBinding.Create<$1L, I$2LHandler>(
              $3L, $4L, $5L, Decode$2L, static (handler, message, ct) => message switch
              {
                  ${6L|}
                  $1L.Unknown unknown => handler.HandleUnknownAsync(unknown.Tag, ct),
                  _ => throw new System.InvalidOperationException("Unknown $2L event."),
              });
          """,
          typeName(channel.union()),
          channel.name(),
          literal(contract.service().getId().toString()),
          literal(channel.union().getId().toString()),
          literal(channel.address().name()),
          channel.events().stream()
              .map(
                  event ->
                      String.format(
                          "%s.%s e => handler.Handle%sAsync(e.Value, ct),",
                          typeName(channel.union()), event.name(), event.name()))
              .collect(Collectors.joining("\n")));
    }
    writer.closeBlock("}");

    writeExtensions(
        writer,
        w -> {
          if (!contract.commands().isEmpty()) {
            writeSingleton(w, service + "Client", clientArguments());
          }
          for (Channel channel : contract.channels()) {
            writeConsumerRegistration(
                w,
                channel.name() + "Consumer",
                List.of(service + "Messaging." + channel.name() + "Inbound"));
          }
        });
  }

  private void writeCommandClient(CSharpWriter writer) {
    boolean sends = contract.commands().stream().anyMatch(c -> c.output().isEmpty());
    boolean requests = contract.commands().stream().anyMatch(c -> c.output().isPresent());
    StringBuilder parameters = new StringBuilder();
    StringBuilder fields = new StringBuilder();
    if (sends) {
      parameters.append("IMessageSender sender");
      fields.append(
          """
          private readonly IMessageSender _sender =
              sender ?? throw new System.ArgumentNullException(nameof(sender));
          """);
    }
    if (requests) {
      parameters.append(sends ? ", " : "").append("IMessageRequestSender requests");
      fields.append(
          """
          private readonly IMessageRequestSender _requests =
              requests ?? throw new System.ArgumentNullException(nameof(requests));
          """);
    }
    String signatures =
        contract.commands().stream()
            .map(
                c ->
                    String.format(
                        "%s %sAsync(%s command, %s cancellationToken = default);",
                        replyTask(c), c.name(), typeName(c.input()), CANCELLATION_TOKEN))
            .collect(Collectors.joining("\n"));
    String methods =
        contract.commands().stream()
            .map(
                c ->
                    String.format(
                        """
                        public %s %sAsync(%s command, %s cancellationToken = default) =>
                            %s(%sMessaging.%sOutbound, command, cancellationToken);
                        """,
                        replyTask(c),
                        c.name(),
                        typeName(c.input()),
                        CANCELLATION_TOKEN,
                        c.output().isPresent() ? "_requests.RequestAsync" : "_sender.SendAsync",
                        service,
                        c.name()))
            .collect(Collectors.joining("\n"));
    writer.write(
        """
        public interface I$1LClient
        {
            ${2L|}
        }

        public sealed class $1LClient($3L) : I$1LClient
        {
            ${4L|}

            ${5L|}
        }
        """,
        service,
        signatures,
        parameters,
        fields.toString().strip(),
        methods);
  }

  private void writeChannelHandler(CSharpWriter writer, Channel channel) {
    String methods =
        channel.events().stream()
            .map(
                event ->
                    String.format(
                        "%s Handle%sAsync(%s @event, %s cancellationToken = default);",
                        TASK, event.name(), typeName(event.shape()), CANCELLATION_TOKEN))
            .collect(Collectors.joining("\n"));
    writer.write(
        """
        public interface I$1LHandler
        {
            ${2L|}

            /// <summary>Handles an event this contract version does not know. Skips it by default.</summary>
            $3L HandleUnknownAsync(string type, $4L cancellationToken = default) =>
                $3L.CompletedTask;
        }
        """,
        channel.name(),
        methods,
        TASK,
        CANCELLATION_TOKEN);
  }

  // Inbounds and outbounds.

  private String outboundType(Command command) {
    String input = typeName(command.input());
    return command
        .output()
        .map(o -> "OutboundBinding<" + input + ", " + typeName(o) + ">")
        .orElse("OutboundBinding<" + input + ">");
  }

  private String commandInbound(Command command) {
    String input = typeName(command.input());
    String handler = "I" + command.name() + "Handler";
    String handle = "static (handler, command, ct) => handler.HandleAsync(command, ct)";
    return command
        .output()
        .map(
            o ->
                String.format(
                    "InboundBinding.CreateReply<%s, %s, %s>(%s, Decode%s, Encode%sReply, %s)",
                    input,
                    typeName(o),
                    handler,
                    facts(command),
                    command.name(),
                    command.name(),
                    handle))
        .orElse(
            String.format(
                "InboundBinding.Create<%s, %s>(%s, Decode%s, %s)",
                input, handler, facts(command), command.name(), handle));
  }

  /** The service id, operation id and address arguments of a command's inbound and outbound. */
  String facts(Command command) {
    return String.join(
        ", ",
        literal(contract.service().getId().toString()),
        literal(command.operation().getId().toString()),
        literal(command.address().name()));
  }

  /** The service id, channel id and address arguments of a channel's inbound and outbound. */
  String facts(Channel channel) {
    return String.join(
        ", ",
        literal(contract.service().getId().toString()),
        literal(channel.union().getId().toString()),
        literal(channel.address().name()));
  }

  // Dependency injection.

  // Registrations are generated whatever NSmithy's generateDependencyInjection says: consumers can
  // only be hosted through them, since the bindings they pass are internal.
  private void writeExtensions(CSharpWriter writer, Consumer<CSharpWriter> registrations) {
    writer.addImport("Microsoft.Extensions.DependencyInjection");
    writer.addImport("Microsoft.Extensions.DependencyInjection.Extensions");
    writer.write("public static partial class $LMessagingExtensions", service);
    writer.openBlock("{");
    registrations.accept(writer);
    writer.closeBlock("}");
  }

  private void writeSingleton(CSharpWriter writer, String type, String arguments) {
    writer.write(
        """
        public static IServiceCollection Add$1L(this IServiceCollection services)
        {
            services.TryAddSingleton<I$1L>(provider =>
            {
                var sender = provider.GetRequiredService<$2L>();
                return new $1L($3L);
            });
            return services;
        }
        """,
        type,
        senderType(),
        arguments);
  }

  // The client takes a sender for one-way commands and a request sender for request/reply ones.
  private String clientArguments() {
    boolean sends = contract.commands().stream().anyMatch(c -> c.output().isEmpty());
    boolean requests = contract.commands().stream().anyMatch(c -> c.output().isPresent());
    return sends && requests ? "sender, sender" : "sender";
  }

  private void writeConsumerRegistration(CSharpWriter writer, String name, List<String> inbounds) {
    String parameters = consumerParameters();
    writer.write(
        """
        public static IServiceCollection Add$L(this IServiceCollection services$L) =>
            $L;
        """,
        name,
        parameters.isEmpty() ? "" : ", " + parameters,
        consumerRegistration(inbounds));
  }

  // Helpers.

  private String replyTask(Command command) {
    return command.output().map(o -> TASK + "<" + typeName(o) + ">").orElse(TASK);
  }

  String typeName(Shape shape) {
    return context.symbolProvider().toSymbol(shape).getFullName();
  }

  String schema(CSharpWriter writer, Shape shape) {
    return SchemaGenerator.schemaClassName(writer, context, shape);
  }

  /** The name of the codec field for a payload type. */
  String codec(Shape shape) {
    return CSharpNaming.typeName(shape.getId().getName()) + "Codec";
  }

  static String literal(String value) {
    return CSharpNaming.formatString(value);
  }
}
