const { execSync, spawn } = require('child_process');
const os = require('os');

try {
  execSync('rustc --version', { stdio: 'ignore' });
  console.log('✅ Rust is already installed.');
} catch (error) {
  console.warn('⚠️ Rust not found. Installing...');

  if (os.platform() === 'win32') {
    // Windows installation using rustup-init.exe
    const installRust = spawn('powershell.exe', ['-Command', 'Invoke-WebRequest -Uri https://win.rustup.rs -OutFile rustup-init.exe; ./rustup-init.exe -y']);
    
    installRust.stdout.on('data', (data) => console.log(data.toString()));
    installRust.stderr.on('data', (data) => console.error(data.toString()));

    installRust.on('close', (code) => {
      if (code !== 0) {
        console.error('🚨 Failed to install Rust automatically on Windows. Please install it manually from https://rustup.rs.');
      } else {
        console.log('✅ Rust installed successfully!');
      }
    });
  } else {
    // For Unix-based systems
    try {
      execSync('curl --proto \'=https\' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y', { stdio: 'inherit' });
      console.log('✅ Rust installed successfully!');
    } catch (installError) {
      console.error('🚨 Failed to install Rust automatically. Please install it manually from https://rustup.rs.');
    }
  }
}
