const dgram = require('dgram');

// Test UDP connection dari Railway
function testUDP() {
  console.log('Testing UDP outbound from Railway...');
  
  const client = dgram.createSocket('udp4');
  
  // Test DNS query (UDP port 53)
  const dnsQuery = Buffer.from([
    0x00, 0x00, // ID
    0x01, 0x00, // Flags
    0x00, 0x01, // Questions
    0x00, 0x00, // Answers
    0x00, 0x00, // Authority
    0x00, 0x00, // Additional
    // Query: google.com
    0x06, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x03, 0x63, 0x6f, 0x6d, 0x00,
    0x00, 0x01, // Type A
    0x00, 0x01  // Class IN
  ]);
  
  client.send(dnsQuery, 53, '8.8.8.8', (err) => {
    if (err) {
      console.error('UDP send error:', err);
      client.close();
      return;
    }
    console.log('DNS query sent to 8.8.8.8:53');
  });
  
  client.on('message', (msg, rinfo) => {
    console.log(`Received UDP response from ${rinfo.address}:${rinfo.port}`);
    console.log(`Response length: ${msg.length} bytes`);
    client.close();
    process.exit(0);
  });
  
  client.on('error', (err) => {
    console.error('UDP client error:', err);
    client.close();
    process.exit(1);
  });
  
  // Timeout
  setTimeout(() => {
    console.log('UDP test timeout');
    client.close();
    process.exit(1);
  }, 5000);
}

testUDP();
