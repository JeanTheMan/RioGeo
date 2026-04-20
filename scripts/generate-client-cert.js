const path = require('path');
const { Proxy } = require('http-mitm-proxy');

const sslCaDir = path.resolve(process.env.SSL_CA_DIR || path.join(__dirname, '..', '.mitm-proxy-client'));
const caCertPath = path.join(sslCaDir, 'certs', 'ca.pem');

function nowIso() {
  return new Date().toISOString();
}

function main() {
  const proxy = new Proxy();

  proxy.listen({
    port: 0,
    host: '127.0.0.1',
    sslCaDir,
    forceSNI: true
  });

  setTimeout(() => {
    proxy.close();
    console.log(`${nowIso()} | initialized CLIENT MITM CA at: ${caCertPath}`);
    console.log(`${nowIso()} | import this CA into Trusted Root Certification Authorities before capturing HTTPS/WSS`);
  }, 500);
}

main();
