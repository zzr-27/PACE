//get nginx pid
const {isIPv4, isIPv6} = require('net');
const os = require('os-utils');
const fs = require('fs');
const WebSocket = require('ws');
const {exec, spawn} = require("child_process");


const too_many_status = {
    "cubictcp_cwnd_event": -1,
    "tcp_parse_md5sig_option": -1,
    "tcp_rcv_established": -1,
    "tcp_queue_rcv": -1,
    "tcp_event_data_recv": -1,
    "tcp_data_ready": -1,
    "__tcp_ack_snd_check": -1,
    "tcp_rcv_space_adjust": -1,
    "tcp_rbtree_insert": -1,
    "tcp_rearm_rto": -1,
    "tcp_check_space": -1,
    "tcp_try_coalesce": -1,
    "tcp_ack": -1,
    "tcp_ack_tstamp": -1,
    "cubictcp_acked": -1,
    "tcp_urg": -1,
    "tcp_data_queue": -1,
    "tcp_try_rmem_schedule": -1,
    "tcp_parse_options": -1

}
// Define global variables to record the amount of data
let originalDataSize = 0;
let totalDataSize = 0;


function getNginxPid() {
    const {exec} = require('child_process');

//  Get Nginx main process PID
    exec("ps aux | grep 'apache2' | grep -v grep | awk '{print $2}'", (error, stdout, stderr) => {
        if (error) {
            console.error(`执行命令时发生错误：${error.message}`);
            return;
        }
        if (stderr) {
            console.error(`命令执行产生了错误输出：${stderr}`);
            return;
        }

// Extract the PID of the Nginx process
        let nginxPids = stdout.trim().split(/\s+/);
        console.log(`Nginx 所有进程的 PID 是：${nginxPids}`);

        // 存储 PID 到全局变量中
        global.nginxPids = nginxPids;
    });
    return global.nginxPids
}

getNginxPid();
var find = runStap();
const readline = require('readline');

function runStap() {
//systemtap
    const {spawn} = require('child_process');
    const cmd = 'sudo'; 
    const find = spawn(cmd, ['/home/cc/systemtap-4.9/stap', '/home/cc/systemtap-test/socket-trace.stp']);

    //const find = exec(cmd);
    find.stdout.setEncoding('utf8');

    return find;
}

// Listen for the closure event of the child process
find.on('close', (code) => {
    console.log(`exit ${code}`);
    find = runStap();
});
find.on('error', (error) => {
    console.error('error：', error);
    find=runStap()
});


//ws
const server = new WebSocket.Server({port: 3000});
let handler = monitorCpuUsageToFile('cpu.csv', 100);

server.on('connection', (socket, request) => {
    let lastCwnd = 0;
    let lastCwndTime = 0
    let clientIP = request.connection.remoteAddress;

    // Check if the IP address is IPv6
    if (isIPv6(clientIP)) {
        // Extract the IPv4 address from the IPv6 address
        clientIP = clientIP.replace(/^.*:/, '');
    }
    clientIP = clientIP.trim();
    let messageCounter = 0; 
    console.log('Client connected IP:', clientIP);

    originalDataSize = 0;
    totalDataSize = 0;

    // filename = "serverMessage.txt"
    // fs.writeFile(filename, "START RECORD\n", (err) => {
    //     if (err) throw err;
    // });

    // read interface
    let rl = readline.createInterface({
        input: find.stdout
    });
    // read interface
    rl.on('line', (line) => {
        const kind_part = line.substring(0, line.indexOf("->"));

        if (kind_part.includes("input") || kind_part.includes("cubic")) {
            // line = line.substring(line.indexOf(' ') + 1);
            for (const pid of global.nginxPids) {
                if (kind_part.includes(pid)) {
                    const timestamp = new Date().getTime();
                    line = line.substring(line.indexOf("->") + 2).trim();
                    if (line in too_many_status) {
                        return;
                    }
                    // Construct the message object
                    const messageId = ++messageCounter;
                    const serverMessage = {
                        type: 'tcp_func_call', data: {
                            messageId, timestamp, message: line
                        }
                    };
                    // Send the message to the client
                    let serverMessageStr = JSON.stringify(serverMessage);
                    originalDataSize += serverMessageStr.length;
                    const tcpHeaderSize = 20; 
                    totalDataSize += serverMessageStr.length + tcpHeaderSize;

                    socket.send(serverMessageStr);
                    break;
                }
            }
        } else if (kind_part.includes("cwnd")) {
            line = line.substring(line.indexOf("->") + 2).trim();
            const timestamp = new Date().getTime();
            if (Math.abs(timestamp - lastCwndTime) < 10) return;
            else lastCwndTime = timestamp;

            // Extract the IP address
            const ipRegex = /ip: \[(\d+\.\d+\.\d+\.\d+)\]/;
            const ipMatch = line.match(ipRegex);
            const ipAddress = ipMatch ? ipMatch[1] : "IP_FAILED";
            if (ipAddress !== clientIP) {
                return;
            }

            // Extract the timestamp
            const timestampRegex = /\[(\d+)\]/;
            const timestampMatch = line.match(timestampRegex);
            const timestampValue = timestampMatch ? parseInt(timestampMatch[1]) * 1000 : timestamp;

            // Extract cur_cwnd
            const curCwndRegex = /cur_cwnd: \[(\d+)\]/;
            const curCwndMatch = line.match(curCwndRegex);
            const curCwnd = curCwndMatch ? parseInt(curCwndMatch[1]) : -1;

            if (lastCwnd === curCwnd) return;
            else lastCwnd = curCwnd;

            // Extract last_max_cwnd
            const lastMaxCwndRegex = /last_max_cwnd: \[(\d+)\]/;
            const lastMaxCwndMatch = line.match(lastMaxCwndRegex);
            const lastMaxCwnd = lastMaxCwndMatch ? parseInt(lastMaxCwndMatch[1]) : -1;

            // Extract ssthresh
            const ssthreshRegex = /ssthresh: \[(\d+)\]/;
            const ssthreshMatch = line.match(ssthreshRegex);
            const ssthresh = ssthreshMatch ? parseInt(ssthreshMatch[1]) : -1;

            // Extract rtt
            const rttRegex = /rtt: \[(\d+)\]/;
            const rttMatch = line.match(rttRegex);
            const rtt = rttMatch ? parseInt(rttMatch[1]) : -1;

            // check if any var is -1
            if (curCwnd === -1 || lastMaxCwnd === -1 || ssthresh === -1 || rtt === -1) {
                return;
            }
            const messageId = ++messageCounter;

            const serverMessage = {
                type: 'cwnd_change', data: {
                    messageId, timestamp: timestampValue, curCwnd, lastMaxCwnd, ssthresh, rtt
                }
            };
            socket.send(JSON.stringify(serverMessage));
        } else if (kind_part.includes("ca_state")) {
            // line = line.substring(line.indexOf(' ') + 1).trim();
            for (const pid of global.nginxPids) {
                if (kind_part.includes(pid)) {
                    // console.log(line);
                    const messageId = ++messageCounter;
                    const timestamp = new Date().getTime();
                    line = line.substring(line.indexOf("->") + 2).trim();

                    // Extract ca_state
                    const caStateRegex = /ca_state=0x(\w+)/;
                    const caStateMatch = line.match(caStateRegex);
                    const caStateValue = caStateMatch ? parseInt(caStateMatch[1], 16) : -1;
                    if (caStateValue === -1) return;

                    // Extract timestamp
                    const timestampRegex = /\[(\d+)\]/;
                    const timestampMatch = line.match(timestampRegex);
                    const timestampValue = timestampMatch ? parseInt(timestampMatch[1]) : timestamp;

                    const serverMessage = {
                        type: 'ca_state_change', data: {
                            messageId, timestamp: timestampValue, caState: caStateValue
                        }
                    };
                    // console.log(serverMessage)

                    socket.send(JSON.stringify(serverMessage));
                    break;
                }
            }
        }

    });

    // Listen for messages sent by the client
    socket.on('message', (data) => {
        const clientMessage = JSON.parse(data);

        if (clientMessage.type === 'clientResponse') {
            // Handle the response messages from the client
            const messageId = clientMessage.data.messageId;
            const clientTimestamp = clientMessage.data.sendingTimestamp;

            // RTT
            const roundTripTime = new Date().getTime() - clientTimestamp;

            // Print Log
            //console.log(`Received client response for message ${messageId}: Round-trip time: ${roundTripTime} ms`);
            const feedbackMessage = {
                type: 'feedback', data: {
                    messageId, roundTripTime
                }
            };
            socket.send(JSON.stringify(feedbackMessage));
        } else if (clientMessage.type === 'clockSync') {
            const messageId = clientMessage.messageId;
            const serverTimestamp = new Date().getTime();
            const clientTimestamp = clientMessage.clientTimestamp;

            const clockSyncRespMessage = {
                type: 'clockSyncResp', data: {
                    messageId, serverTimestamp, clientTimestamp
                }
            };
            socket.send(JSON.stringify(clockSyncRespMessage));
        } else {
            // Handle other types of messages
            console.log(`Received from client: ${data}`);
        }
    });
    // Stop sending messages when the client disconnects
    socket.on('close', () => {
        // clearInterval(handler);
        console.log('Client disconnected');
    });
});


function monitorCpuUsageToFile(filePath, interval = 500) {
    const csvHeader = 'Timestamp,CPU_Usage,Original_Data_Size,Total_Data_Size\n';

    
    fs.writeFile(filePath, csvHeader, (err) => {
        if (err) {
            console.error('table header cannot be written to the file:', err);
        } else {
            console.log(`create ${filePath} file and write it into the table header`);
        }
    });

    function monitorCpuUsage() {
        os.cpuUsage((usage) => {
            const timestamp = new Date().getTime();

            // construct the data rows of the record
            const dataRow = `${timestamp},${(usage * 100).toFixed(2)},${originalDataSize},${totalDataSize}\n`;

            fs.appendFile(filePath, dataRow, (err) => {
                if (err) {
                    console.error('Cannot be written to the file:', err);
                }
            });
        });
    }

    // output the CPU utilization rate and data volume to the specified file at specified time intervals
    let handler = setInterval(monitorCpuUsage, interval);
    return handler;
}
