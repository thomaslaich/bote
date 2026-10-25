{ pkgs, lib, ... }:
{
  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk21;
    gradle.enable = true;
  };

  languages.dotnet = {
    enable = true;
    package = pkgs.dotnetCorePackages.sdk_10_0-bin;
  };

  packages = [
    pkgs.just
    # Broker for the Redis integration tests (BOTE_TEST_REDIS).
    pkgs.redis
  ];

  env.DOTNET_CLI_TELEMETRY_OPTOUT = "1";
  env.DOTNET_NOLOGO = "1";

  treefmt = {
    enable = true;

    config = {
      programs = {
        csharpier.enable = true;
        google-java-format.enable = true;
        just.enable = true;
        nixfmt.enable = true;
        yamlfmt.enable = true;
      };

      settings = {
        excludes = [
          ".devenv/**"
          ".direnv/**"
          ".git/**"
          "bin/**"
          "**/bin/**"
          "**/obj/**"
          "dotnet/artifacts/**"
        ];

        formatter.csharpier.includes = [
          "*.cs"
          "*.csproj"
          "*.slnx"
          "Directory.Packages.props"
          "Directory.Build.props"
        ];
      };
    };
  };

  tasks."devenv:treefmt:run".exec = lib.mkForce null;
}
