const { execSync, spawn } = require('child_process');
const os = require('os');
const fs = require('fs');
const path = require('path');

function configureRustPath() {
  const platform = os.platform();
  const homeDir = os.homedir();
  const envCmd = 'source $HOME/.cargo/env';

  if (platform === 'win32') {
    console.log("🚀 Rust installed on Windows. If 'cargo' isn't available, please restart your terminal.");
  } else {
    const shell = process.env.SHELL;
    let configFile;

    if (shell.includes('zsh')) {
      configFile = path.join(homeDir, '.zshrc');
    } else if (shell.includes('bash')) {
      configFile = path.join(homeDir, '.bashrc');
    } else {
      console.error("Unsupported shell. Please add Rust to your PATH manually.");
      return;
    }

    try {
      if (fs.existsSync(configFile)) {
        const content = fs.readFileSync(configFile, 'utf-8');
        if (!content.includes(envCmd)) {
          fs.appendFileSync(configFile, `\n${envCmd}\n`);
          console.log(`🚀 Added Rust path to ${configFile}. Please restart your terminal.`);
        }
      }
    } catch (error) {
      console.error(`🚨 Failed to update PATH automatically. Please add Rust to your PATH by adding 'source $HOME/.cargo/env' in your shell config file (e.g., .zshrc or .bashrc).`);
    }
  }
}

try {
  execSync('rustc --version', { stdio: 'ignore' });
  console.log('✅ Rust is already installed.');
} catch (error) {
  console.warn('⚠️ Rust not found and is required to build HyperionDB. Rust will be installed automatically. This should take less than 5 minutes.');

  if (os.platform() === 'win32') {
    const installRust = spawn(
      'powershell.exe',
      [
        '-Command',
        'Invoke-WebRequest -Uri https://win.rustup.rs -OutFile rustup-init.exe; ./rustup-init.exe -y *>&1 | Out-Null'
      ],
      { stdio: 'ignore' }
    );

    installRust.on('close', (code) => {
      if (code !== 0) {
        console.error(
          '🚨 Failed to install Rust automatically on Windows. Please install it manually from https://rustup.rs.'
        );
      } else {
        console.log('✅ Rust installed successfully!');
      }
    });
  } else {
    try {
      execSync(
        'curl --proto \'=https\' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
        { stdio: 'ignore' }
      );
      console.log('✅ Rust installed successfully!');
      configureRustPath(); // Add PATH configuration for Unix-based systems
    } catch (installError) {
      console.error(
        '🚨 Failed to install Rust automatically. Please install it manually from https://rustup.rs.'
      );
    }
  }
}
