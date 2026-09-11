let clusterState = { nodes: {} };
let selectedNodeId = null;

// Setup WebSocket
const ws = new WebSocket(`ws://${window.location.host}/ws`);

ws.onopen = () => appendLog('info', 'WebSocket connected to Raft Monitor');
ws.onclose = () => appendLog('error', 'WebSocket disconnected');
ws.onerror = (err) => appendLog('error', `WebSocket error: ${err}`);

ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    
    if (msg.event === 'state_change') {
        const state = msg.data;
        clusterState.nodes[state.node_id] = state;
        updateNodeUI(state.node_id, state);
        if (state.role === 'LEADER') {
            updateGlobalState(state.kv_store);
        }
        if (selectedNodeId === state.node_id) {
            updateSelectedNodePanel();
        }
    } else if (msg.event === 'rpc') {
        animatePacket(msg.data);
    } else if (msg.event === 'partition_change' || msg.event === 'cluster_snapshot') {
        clusterState = msg.data;
        const container = document.getElementById('node-container');
        const whiteboard = document.getElementById('whiteboard');
        
        Object.keys(clusterState.nodes).forEach(id => {
            let el = document.getElementById(`node-${id}`);
            if (!el) {
                const nodeRadius = 55;
                const cx = (whiteboard.offsetWidth / 2) - nodeRadius;
                const cy = (whiteboard.offsetHeight / 2) - nodeRadius;
                const jitterX = Math.random() * 60 - 30;
                const jitterY = Math.random() * 60 - 30;
                el = createNodeElement(id, cx + jitterX, cy + jitterY);
                container.appendChild(el);
            }
            updateNodeUI(id, clusterState.nodes[id]);
        });
        drawNetworkLines();
    }
};

// Initial state fetch
async function fetchState() {
    try {
        const res = await fetch('/api/cluster');
        const data = await res.json();
        clusterState = data;
        
        const container = document.getElementById('node-container');
        container.innerHTML = ''; // clear

        // Setup nodes in pentagon (will be draggable later)
        const nodeIds = Object.keys(data.nodes);
        const whiteboard = document.getElementById('whiteboard');
        const nodeRadius = 55; // half of 110px node size
        const cx = (whiteboard.offsetWidth / 2) - nodeRadius;
        const cy = (whiteboard.offsetHeight / 2) - nodeRadius;
        const radius = Math.min(whiteboard.offsetWidth / 2, whiteboard.offsetHeight / 2) - nodeRadius - 10;

        nodeIds.forEach((id, i) => {
            const angle = (Math.PI * 2 * i) / nodeIds.length - Math.PI / 2;
            const x = cx + radius * Math.cos(angle);
            const y = cy + radius * Math.sin(angle);
            
            const nodeEl = createNodeElement(id, x, y);
            container.appendChild(nodeEl);
            updateNodeUI(id, data.nodes[id]);
        });
        
        drawNetworkLines();
    } catch (e) {
        appendLog('error', `Failed to fetch state: ${e.message}`);
    }
}

function createNodeElement(id, x, y) {
    const div = document.createElement('div');
    div.className = 'node';
    div.id = `node-${id}`;
    div.style.left = `${x}px`;
    div.style.top = `${y}px`;

    div.innerHTML = `
        <div class="node-id">${id}</div>
        <div class="node-role" id="role-${id}">INIT</div>
        <div class="node-term" id="term-${id}">Term: 0</div>
        <div class="node-commit" id="commit-${id}">Commit: 0</div>
    `;

    setupDraggable(div, id);
    return div;
}

function updateNodeUI(id, state) {
    const el = document.getElementById(`node-${id}`);
    if (!el) return;

    const roleEl = document.getElementById(`role-${id}`);
    const termEl = document.getElementById(`term-${id}`);
    const commitEl = document.getElementById(`commit-${id}`);

    const isDead = state.role === 'DEAD' || state.is_running === false;
    const effectiveRole = isDead ? 'DEAD' : state.role;

    // Emojis for status
    const statusEmoji = isDead ? '💀' : '💚';

    roleEl.innerText = `${statusEmoji} ${effectiveRole}`;
    roleEl.className = `node-role role-${effectiveRole}`;

    if (isDead) {
        el.classList.add('offline');
        termEl.innerText = 'OFFLINE';
        commitEl.innerText = '';
    } else {
        el.classList.remove('offline');
        termEl.innerText = `Term: ${state.term}`;
        commitEl.innerText = `Commit: ${state.commit_index}`;
    }
    
    // Select visual feedback
    if (selectedNodeId === id) {
        el.classList.add('selected');
    } else {
        el.classList.remove('selected');
    }
}

// Drag & Drop & Selection
function setupDraggable(el, id) {
    let isDragging = false;
    let offsetX = 0, offsetY = 0;

    el.addEventListener('mousedown', (e) => {
        isDragging = true;
        const rect = el.getBoundingClientRect();
        const parentRect = el.parentElement.getBoundingClientRect();
        
        // Calculate offset relative to the node
        offsetX = e.clientX - rect.left;
        offsetY = e.clientY - rect.top;
        
        el.style.zIndex = 100;
        
        // Handle selection
        selectNode(id);
    });

    document.addEventListener('mousemove', (e) => {
        if (!isDragging) return;
        
        const parentRect = document.getElementById('whiteboard').getBoundingClientRect();
        
        // Calculate new position relative to whiteboard
        let newX = e.clientX - parentRect.left - offsetX;
        let newY = e.clientY - parentRect.top - offsetY;
        
        // Constrain to whiteboard
        newX = Math.max(0, Math.min(newX, parentRect.width - el.offsetWidth));
        newY = Math.max(0, Math.min(newY, parentRect.height - el.offsetHeight));

        el.style.left = `${newX}px`;
        el.style.top = `${newY}px`;
        drawNetworkLines();
    });

    document.addEventListener('mouseup', () => {
        if (isDragging) {
            isDragging = false;
            el.style.zIndex = 2;
        }
    });
}

function selectNode(id) {
    // Remove selected class from previous
    if (selectedNodeId) {
        const prevEl = document.getElementById(`node-${selectedNodeId}`);
        if (prevEl) prevEl.classList.remove('selected');
    }
    
    selectedNodeId = id;
    
    // Add selected class to new
    const newEl = document.getElementById(`node-${id}`);
    if (newEl) newEl.classList.add('selected');
    
    updateSelectedNodePanel();
}

function updateSelectedNodePanel() {
    const idSpan = document.getElementById('sel-node-id');
    const roleSpan = document.getElementById('sel-node-role');
    const btnKill = document.getElementById('btn-sel-kill');
    const btnRevive = document.getElementById('btn-sel-revive');

    if (!selectedNodeId || !clusterState.nodes[selectedNodeId]) {
        idSpan.innerText = 'None';
        roleSpan.innerText = '-';
        btnKill.disabled = true;
        btnRevive.disabled = true;
        return;
    }

    const state = clusterState.nodes[selectedNodeId];
    idSpan.innerText = selectedNodeId;
    
    const isDead = state.role === 'DEAD' || state.is_running === false;
    roleSpan.innerText = isDead ? 'DEAD' : state.role;
    
    if (isDead) {
        btnKill.disabled = true;
        btnRevive.disabled = false;
    } else {
        btnKill.disabled = false;
        btnRevive.disabled = true;
    }
}

// Master Control Actions
document.getElementById('btn-sel-kill').addEventListener('click', () => {
    if (selectedNodeId) {
        appendLog('warn', `Killing node ${selectedNodeId}`);
        apiPost(`/nodes/${selectedNodeId}/kill`);
    }
});

document.getElementById('btn-sel-revive').addEventListener('click', () => {
    if (selectedNodeId) {
        appendLog('info', `Reviving node ${selectedNodeId}`);
        apiPost(`/nodes/${selectedNodeId}/revive`);
    }
});


// Network Visualization
function drawNetworkLines() {
    const svg = document.getElementById('network-lines');
    svg.innerHTML = '';
    const nodeIds = Object.keys(clusterState.nodes || {});
    const partitions = clusterState.partitions || {};
    
    for (let i=0; i<nodeIds.length; i++) {
        for (let j=i+1; j<nodeIds.length; j++) {
            const id1 = nodeIds[i];
            const id2 = nodeIds[j];
            const el1 = document.getElementById(`node-${id1}`);
            const el2 = document.getElementById(`node-${id2}`);
            if (!el1 || !el2) continue;

            const node1State = clusterState.nodes[id1];
            const node2State = clusterState.nodes[id2];
            
            const isDead1 = !node1State || node1State.role === 'DEAD' || node1State.is_running === false;
            const isDead2 = !node2State || node2State.role === 'DEAD' || node2State.is_running === false;

            if (isDead1 || isDead2) continue; // Cut connection to dead nodes!

            // If a partition exists between these two, do not draw the line
            const isPartitioned = (partitions[id1] && partitions[id1].includes(id2)) ||
                                  (partitions[id2] && partitions[id2].includes(id1));
            
            if (isPartitioned) continue; // Cut the connection!

            const x1 = parseFloat(el1.style.left) + el1.offsetWidth/2;
            const y1 = parseFloat(el1.style.top) + el1.offsetHeight/2;
            const x2 = parseFloat(el2.style.left) + el2.offsetWidth/2;
            const y2 = parseFloat(el2.style.top) + el2.offsetHeight/2;

            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', x1);
            line.setAttribute('y1', y1);
            line.setAttribute('x2', x2);
            line.setAttribute('y2', y2);
            line.setAttribute('stroke', '#151617');
            line.setAttribute('stroke-width', '2');
            line.setAttribute('stroke-dasharray', '5,5');
            line.setAttribute('opacity', '0.2');
            svg.appendChild(line);
        }
    }
}

function animatePacket(rpc) {
    const senderEl = document.getElementById(`node-${rpc.sender}`);
    const receiverEl = document.getElementById(`node-${rpc.receiver}`);
    if (!senderEl || !receiverEl) return;

    const x1 = parseFloat(senderEl.style.left) + senderEl.offsetWidth/2;
    const y1 = parseFloat(senderEl.style.top) + senderEl.offsetHeight/2;
    const x2 = parseFloat(receiverEl.style.left) + receiverEl.offsetWidth/2;
    const y2 = parseFloat(receiverEl.style.top) + receiverEl.offsetHeight/2;

    const pkt = document.createElement('div');
    pkt.className = `packet ${rpc.type.toLowerCase()}`;
    pkt.style.left = `${x1}px`;
    pkt.style.top = `${y1}px`;

    document.getElementById('packets-container').appendChild(pkt);

    // trigger reflow
    pkt.getBoundingClientRect();

    pkt.style.left = `${x2}px`;
    pkt.style.top = `${y2}px`;

    setTimeout(() => {
        if (pkt.parentElement) pkt.remove();
    }, 400);
}


// Helpers
async function apiPost(endpoint, body={}) {
    try {
        const res = await fetch(`/api${endpoint}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        const data = await res.json();
        return data;
    } catch (e) {
        appendLog('error', `API Error (${endpoint}): ${e.message}`);
    }
}

function appendLog(level, msg) {
    const logs = document.getElementById('system-logs');
    const div = document.createElement('div');
    div.className = 'log-line';
    const time = new Date().toISOString().split('T')[1].substring(0,8);
    div.innerHTML = `<span class="log-time">${time}</span> <span class="log-level-${level}">[${level.toUpperCase()}]</span> ${msg}`;
    logs.appendChild(div);
    logs.scrollTop = logs.scrollHeight;
}

function updateGlobalState(kv) {
    const el = document.getElementById('global-kv');
    el.innerText = JSON.stringify(kv, null, 2);
}

// UI Buttons
document.getElementById('btn-submit').addEventListener('click', () => {
    const key = document.getElementById('cmd-key').value;
    const val = document.getElementById('cmd-val').value;
    if (key && val) {
        appendLog('info', `Sending client command: ${key}=${val}`);
        apiPost('/command', {op: 'SET', key: key, val: val});
    }
});

// Network Chaos
document.getElementById('btn-isolate').addEventListener('click', () => {
    const leaderId = Object.keys(clusterState.nodes).find(id => clusterState.nodes[id].role === 'LEADER');
    if (!leaderId) {
        appendLog('warn', 'No leader to isolate!');
        return;
    }
    const followers = Object.keys(clusterState.nodes).filter(id => id !== leaderId);
    appendLog('warn', `Isolating leader ${leaderId}`);
    apiPost('/partition', { group_a: [leaderId], group_b: followers });
});

document.getElementById('btn-partition').addEventListener('click', () => {
    const nodes = Object.keys(clusterState.nodes);
    const groupA = nodes.slice(0, 3);
    const groupB = nodes.slice(3);
    appendLog('warn', `Partitioning 3/2: ${groupA.length} vs ${groupB.length}`);
    apiPost('/partition', { group_a: groupA, group_b: groupB });
});

document.getElementById('btn-heal').addEventListener('click', () => {
    appendLog('info', 'Healing all network partitions');
    apiPost('/heal');
});

document.getElementById('btn-add-node').addEventListener('click', async () => {
    appendLog('info', 'Spinning up 2 new nodes...');
    await apiPost('/nodes/add');
});

document.getElementById('btn-reset').addEventListener('click', async () => {
    appendLog('error', 'Cluster reset requested! Restarting nodes...');
    await apiPost('/reset');
    window.location.reload();
});

// Init
fetchState();
