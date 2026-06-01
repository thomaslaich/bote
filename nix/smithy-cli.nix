{
  fetchurl,
  lib,
  stdenv,
  unzip,
}:

let
  version = "1.69.0";

  platform =
    if stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isAarch64 then
      {
        name = "darwin-aarch64";
        hash = "c957f4f9ecd070630f514ba3cfff91693f747603f48a61576fab285440d93867";
      }
    else if stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isx86_64 then
      {
        name = "darwin-x86_64";
        hash = "4801bd20a704eaa65200237de26f4df83f6c7fccd559eab0682898e9181ffab5";
      }
    else if stdenv.hostPlatform.isLinux && stdenv.hostPlatform.isAarch64 then
      {
        name = "linux-aarch64";
        hash = "ac9d1b0b8f5e39a951d367b54a2b0330f803cf6d9cd2d10d2b67e67f56c59a1f";
      }
    else if stdenv.hostPlatform.isLinux && stdenv.hostPlatform.isx86_64 then
      {
        name = "linux-x86_64";
        hash = "45db1e239c0aa5fce6706676701c9e4ed09e67cd231222762207dcf384c9ea4e";
      }
    else
      throw "Unsupported platform for smithy-cli: ${stdenv.hostPlatform.system}";
in
stdenv.mkDerivation {
  pname = "smithy-cli";
  inherit version;

  src = fetchurl {
    url = "https://github.com/smithy-lang/smithy/releases/download/${version}/smithy-cli-${platform.name}.zip";
    sha256 = platform.hash;
  };

  nativeBuildInputs = [ unzip ];

  dontBuild = true;

  unpackPhase = ''
    runHook preUnpack
    unzip -q "$src"
    runHook postUnpack
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p "$out/share/smithy-cli" "$out/bin"
    cp -R smithy-cli-${platform.name}/. "$out/share/smithy-cli/"

    if [ -x "$out/share/smithy-cli/bin/smithy" ]; then
      ln -s "$out/share/smithy-cli/bin/smithy" "$out/bin/smithy"
    elif [ -x "$out/share/smithy-cli/smithy" ]; then
      ln -s "$out/share/smithy-cli/smithy" "$out/bin/smithy"
    else
      echo "Could not find smithy executable in Smithy CLI archive" >&2
      find "$out/share/smithy-cli" -maxdepth 3 -type f -o -type l >&2
      exit 1
    fi

    runHook postInstall
  '';

  meta = {
    description = "Command line interface for the Smithy API modeling language";
    homepage = "https://smithy.io/2.0/guides/smithy-cli/cli_installation.html";
    license = lib.licenses.asl20;
    platforms = [
      "aarch64-darwin"
      "x86_64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ];
  };
}
