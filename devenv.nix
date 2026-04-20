{ pkgs, ... }:

{
  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk21;
    gradle.enable = true;
  };

  packages = [ pkgs.just ];
}
