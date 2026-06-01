{ pkgs, lib, ... }:
{
  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk21;
    gradle.enable = true;
  };

  packages = [
    pkgs.just
  ];

  treefmt = {
    enable = true;

    config = {
      programs = {
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
        ];
      };
    };
  };

  tasks."devenv:treefmt:run".exec = lib.mkForce null;
}
