// scripts/check_rust.js
const { execSync } = require('child_process');
const process = require('process');

function checkRust() {
  try {
    execSync('rustc --version', { stdio: 'ignore' });
    console.log('✅ Rust is already installed.');
  } catch (e) {
    console.log('⚠️ Rust not found. Installing...');
    try {
      execSync('curl --proto \'=https\' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y', { stdio: 'inherit' });
      console.log('✅ Rust installed successfully.');
      console.log('Please restart your terminal or shell to refresh the environment variables.');
    } catch (error) {
      console.error('🚨 Failed to install Rust automatically. Please install it manually from https://rustup.rs.');
      process.exit(1); // Aborta la instalación si Rust no pudo instalarse.
    }
  }
}

checkRust();
