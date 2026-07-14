{
  description = "aribcap-db Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        aribcap-db = pkgs.rustPlatform.buildRustPackage {
          pname = "aribcap-db";
          version = "0.1.0";

          src = ./.;

          cargoLock.lockFile = ./Cargo.lock;
        };
      in
      {
        packages = {
          default = aribcap-db;
          inherit aribcap-db;
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            clippy
            nixpkgs-fmt
            pre-commit
            rust-analyzer
            rustc
            rustfmt
            sqlx-cli
          ];
        };
      });
}
