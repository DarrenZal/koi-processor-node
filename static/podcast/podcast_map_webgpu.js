
import * as THREE from 'three';
import WebGPURenderer from 'three/addons/renderers/webgpu/WebGPURenderer.js';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import ThreeForceGraph from 'three-forcegraph';

// Planetary Regeneration Podcast 3D Map (WebGPU Version)
// Based on an earlier podcast map implementation

function logStatus(msg, isError = false) {
    console.log(msg);
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        const line = document.createElement('div');
        line.textContent = msg;
        line.style.fontSize = '14px';
        line.style.marginTop = '5px';
        if (isError) line.style.color = '#ff4444';
        loadingEl.appendChild(line);
    }
}

let graphData = null;
let graph = null;
let scene = null;
let camera = null;
let renderer = null;
let controls = null;
let raycaster = null;
let pointer = null;

let currentEpisode = null;
let currentCluster = null;
let currentClusterLevel = '9';
let activeNode = null;
let autoRotationEnabled = true;
let clickProtectionTimeout = null;
let clusterColors = {};
let cameraFollowEnabled = true;
let userInteractionTimeout = null;
let touchStartTime = 0;
let touchStartPos = { x: 0, y: 0 };
let isTouchGesture = false;

// Helper functions
function hexToRgba(hex, alpha) {
    if (!hex) return `rgba(100, 100, 100, ${alpha})`;
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// Initialize
(async () => {
    try {
        logStatus("Script started...");
        logStatus("Checking WebGPU support...");
        let useWebGPU = false;
        
        if (navigator.gpu) {
            try {
                const adapter = await navigator.gpu.requestAdapter();
                if (adapter) {
                    useWebGPU = true;
                    logStatus("WebGPU supported and available.");
                } else {
                    logStatus("WebGPU adapter not found, falling back to WebGL.");
                }
            } catch (e) {
                logStatus("WebGPU initialization failed, falling back to WebGL.");
            }
        } else {
            logStatus("WebGPU not supported in this browser, falling back to WebGL.");
        }

        logStatus("Fetching graph data...");
        const response = await fetch('/static/podcast/podcast_map_3d.json?v=' + Date.now());
        if (!response.ok) throw new Error(`Failed to load data: ${response.status} ${response.statusText}`);
        
        logStatus("Data fetched. Parsing JSON...");
        graphData = await response.json();
        logStatus(`Loaded: ${graphData.points.length} nodes, ${graphData.links.length} links.`);

        // Build cluster colors map
        if (graphData.clusters_by_level && graphData.clusters_by_level[currentClusterLevel]) {
            graphData.clusters_by_level[currentClusterLevel].forEach(cluster => {
                clusterColors[cluster.id] = cluster.color;
            });
        }

        logStatus(`Initializing 3D Graph (${useWebGPU ? 'WebGPU' : 'WebGL'})...`);
        await initializeGraph(useWebGPU);
        
        logStatus("Done!");
        setupControls();
        setTimeout(() => {
            const loadingEl = document.getElementById('loading');
            if(loadingEl) loadingEl.style.display = 'none';
        }, 1000);
        
    } catch (error) {
        logStatus(error.message, true);
        console.error('Error loading data:', error);
    }
})();

async function initializeGraph(useWebGPU) {
    const container = document.getElementById('graph-container');
    const width = container.clientWidth;
    const height = container.clientHeight;

    // 1. Setup Scene
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0a0e1a);

    // 2. Setup Camera
    camera = new THREE.PerspectiveCamera(75, width / height, 0.1, 100000);
    camera.position.z = 2500;

    // 3. Setup Renderer (WebGPU or WebGL)
    if (useWebGPU) {
        renderer = new WebGPURenderer({ antialias: true });
        if (renderer.init) {
            await renderer.init();
        }
    } else {
        renderer = new THREE.WebGLRenderer({ antialias: true });
    }
    
    renderer.setPixelRatio(window.devicePixelRatio);
    renderer.setSize(width, height);
    container.appendChild(renderer.domElement);

    // 4. Setup Controls
    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.1;
    controls.rotateSpeed = 0.5;
    controls.zoomSpeed = 1.2;

    // 5. Setup Raycaster for interactions
    raycaster = new THREE.Raycaster();
    pointer = new THREE.Vector2();

    // Setup Tooltip
    const tooltip = document.createElement('div');
    tooltip.id = 'graph-tooltip';
    Object.assign(tooltip.style, {
        position: 'absolute',
        padding: '8px',
        background: 'rgba(10, 14, 26, 0.95)',
        border: '1px solid rgba(255, 255, 255, 0.2)',
        color: '#ffffff',
        borderRadius: '6px',
        pointerEvents: 'none',
        display: 'none',
        zIndex: '1000',
        maxWidth: '300px',
        fontSize: '12px',
        backdropFilter: 'blur(4px)'
    });
    document.body.appendChild(tooltip);

    // 6. Setup Graph
    graph = new ThreeForceGraph()
        .graphData(getFilteredData())
        .nodeId('id')
        .nodeColor(node => {
            if (activeNode && node.id === activeNode.id) {
                return '#00ff00';
            }
            const clusterKey = `cluster_${currentClusterLevel}`;
            const nodeColor = clusterColors[node[clusterKey]] || '#666';
            if (currentEpisode) {
                return node.episode_id === currentEpisode ? nodeColor : hexToRgba(nodeColor, 0.2);
            }
            if (currentCluster !== null) {
                return node[clusterKey] === currentCluster ? nodeColor : hexToRgba(nodeColor, 0.2);
            }
            return nodeColor;
        })
        .nodeVal(node => {
            if (activeNode && node.id === activeNode.id) return 20;
            if (currentEpisode) {
                return node.episode_id === currentEpisode ? 12 : 1;
            }
            if (currentCluster !== null) {
                const clusterKey = `cluster_${currentClusterLevel}`;
                return node[clusterKey] === currentCluster ? 8 : 1;
            }
            return 2;
        })
        .nodeOpacity(0.8)
        .nodeResolution(16)
        .linkColor(link => link.color || 'rgba(255, 255, 255, 0.3)')
        .linkOpacity(link => {
            if (currentEpisode) {
                return link.episode_id === currentEpisode ? 0.8 : 0.05;
            }
            if (currentCluster !== null) {
                const key = `cluster_${currentClusterLevel}`;
                const sourceNode = graphData.points.find(n => n.id === link.source.id || n.id === link.source);
                const targetNode = graphData.points.find(n => n.id === link.target.id || n.id === link.target);
                if (sourceNode && targetNode) {
                    const bothInCluster = sourceNode[key] === currentCluster && targetNode[key] === currentCluster;
                    return bothInCluster ? 0.6 : 0.05;
                }
            }
            return link.opacity || 0.3;
        })
        .linkWidth(link => {
            if (currentEpisode) {
                return link.episode_id === currentEpisode ? 3.0 : 0.3;
            }
            if (currentCluster !== null) {
                const key = `cluster_${currentClusterLevel}`;
                const sourceNode = graphData.points.find(n => n.id === link.source.id || n.id === link.source);
                const targetNode = graphData.points.find(n => n.id === link.target.id || n.id === link.target);
                if (sourceNode && targetNode) {
                    const bothInCluster = sourceNode[key] === currentCluster && targetNode[key] === currentCluster;
                    return bothInCluster ? 2.0 : 0.3;
                }
            }
            return 0.8;
        })
        .d3Force('link', d3.forceLink().distance(20).strength(0.1));
        
    // Add graph to scene
    scene.add(graph);
    
    // Add lights
    const ambientLight = new THREE.AmbientLight(0xbbbbbb);
    scene.add(ambientLight);
    const directionalLight = new THREE.DirectionalLight(0xffffff, 0.6);
    directionalLight.position.set(1, 1, 1).normalize();
    scene.add(directionalLight);

    // Setup Interaction Listeners
    setupInteraction(container, tooltip);

    // Start Loop
    if (useWebGPU) {
        renderer.setAnimationLoop(animateWebGPU);
    } else {
        animateWebGL();
    }
    
    // Resize handler
    window.addEventListener('resize', onWindowResize, false);
}

function onWindowResize() {
    const container = document.getElementById('graph-container');
    camera.aspect = container.clientWidth / container.clientHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(container.clientWidth, container.clientHeight);
}

function animateWebGPU() {
    graph.tickFrame();
    controls.update();

    if (autoRotationEnabled && controls) {
        controls.autoRotate = true;
        controls.autoRotateSpeed = 2.0;
    } else {
        controls.autoRotate = false;
    }

    if (renderer.renderAsync) {
        renderer.renderAsync(scene, camera);
    } else {
        renderer.render(scene, camera);
    }
}

function animateWebGL() {
    requestAnimationFrame(animateWebGL);
    graph.tickFrame();
    controls.update();

    if (autoRotationEnabled && controls) {
        controls.autoRotate = true;
        controls.autoRotateSpeed = 2.0;
    } else {
        controls.autoRotate = false;
    }

    renderer.render(scene, camera);
}


// Interaction handling
function setupInteraction(container, tooltip) {
    // We implement raycasting manually since three-forcegraph doesn't have it built-in like 3d-force-graph
    
    container.addEventListener('click', onClick);
    container.addEventListener('mousemove', onMouseMove);
    
    // Disable auto features on interaction
    function disableAutoFeatures() {
        autoRotationEnabled = false;
        cameraFollowEnabled = false;
        if (userInteractionTimeout) clearTimeout(userInteractionTimeout);
        userInteractionTimeout = setTimeout(() => {
            cameraFollowEnabled = true;
        }, 5000);
    }
    
    controls.addEventListener('start', disableAutoFeatures);
    
    let hoveredNode = null;

    function onMouseMove(event) {
        pointer.x = (event.clientX / window.innerWidth) * 2 - 1;
        pointer.y = -(event.clientY / window.innerHeight) * 2 + 1;
        
        // Raycast
        raycaster.setFromCamera(pointer, camera);
        
        // Check intersections with graph nodes
        const intersects = raycaster.intersectObjects(graph.children, true);
        
        let newHoveredNode = null;
        for (let intersect of intersects) {
            if (intersect.object.__data) {
                newHoveredNode = intersect.object.__data;
                break;
            }
        }

        if (newHoveredNode !== hoveredNode) {
            hoveredNode = newHoveredNode;
            document.body.style.cursor = hoveredNode ? 'pointer' : 'default';
            
            if (hoveredNode) {
                const clusterNameKey = `cluster_${currentClusterLevel}_name`;
                tooltip.style.display = 'block';
                tooltip.innerHTML = `<strong>${hoveredNode.episode_title}</strong><br>Topic: ${hoveredNode[clusterNameKey] || 'Unknown'}<br><br>${hoveredNode.text.substring(0, 100)}...`;
            } else {
                tooltip.style.display = 'none';
            }
        }

        if (hoveredNode) {
            tooltip.style.left = (event.clientX + 10) + 'px';
            tooltip.style.top = (event.clientY + 10) + 'px';
        }
    }

    function onClick(event) {
        if (isTouchGesture) return;
        
        pointer.x = (event.clientX / window.innerWidth) * 2 - 1;
        pointer.y = -(event.clientY / window.innerHeight) * 2 + 1;
        
        raycaster.setFromCamera(pointer, camera);
        
        // Find intersections
        // Note: graph is an Object3D. We need to find the nodes.
        // ThreeForceGraph keeps nodes in internal structure but exposes them as children.
        
        // Use a more naive approach: check distance to nodes in screen space or 3D space
        // Or traverse graph children.
        const intersects = raycaster.intersectObjects(graph.children, true); // Recursive
        
        let clickedNode = null;
        
        // Filter for nodes (Mesh)
        for (let intersect of intersects) {
            // Check if it's a node. How to identify? 
            // ThreeForceGraph assigns __data to the object
            if (intersect.object.__data) {
                clickedNode = intersect.object.__data;
                break;
            }
        }
        
        if (clickedNode) {
            handleNodeClick(clickedNode);
        }
    }
}


function getFilteredData() {
    if (!graphData) return { nodes: [], links: [] };
    return { nodes: graphData.points, links: graphData.links };
}

function handleNodeClick(node) {
    if (clickProtectionTimeout) {
        clearTimeout(clickProtectionTimeout);
    }
    clickProtectionTimeout = setTimeout(() => {
        clickProtectionTimeout = null;
    }, 2000);

    console.log('Clicked node:', node);
    activeNode = node;
    currentEpisode = node.episode_id;

    highlightEpisode(node.episode_id);

    // Camera movement - simple implementation
    // Ideally use TWEEN here
    const distance = 200;
    const distRatio = 1 + distance/Math.hypot(node.x, node.y, node.z);
    
    // Move controls target to node
    controls.target.set(node.x, node.y, node.z);
    // Move camera
    camera.position.set(node.x * distRatio, node.y * distRatio, node.z * distRatio);
    
    playAudio(node);
}

function highlightEpisode(episodeId) {
    currentEpisode = episodeId;
    // Trigger update
    graph
        .nodeColor(graph.nodeColor())
        .nodeVal(graph.nodeVal())
        .linkOpacity(graph.linkOpacity())
        .linkWidth(graph.linkWidth());
}

function clearSelection() {
    currentEpisode = null;
    activeNode = null;
    graph
        .nodeColor(graph.nodeColor())
        .nodeVal(graph.nodeVal())
        .linkOpacity(graph.linkOpacity())
        .linkWidth(graph.linkWidth());
}

function playAudio(node) {
    const player = document.getElementById('audio-player');
    const audio = document.getElementById('audio-element');
    const titleEl = document.getElementById('player-episode-title');
    const textEl = document.getElementById('player-text');

    if (!audio.paused) {
        audio.pause();
    }
    audio.currentTime = 0;

    player.classList.add('active');
    titleEl.textContent = node.episode_title;
    textEl.textContent = node.text;

    const audioUrl = node.audio_url || node.episode_url;
    if (audio.src !== audioUrl) {
        audio.src = audioUrl;
    }

    audio.currentTime = node.timestamp;
    
    // Handle playback errors
    const playPromise = audio.play();
    
    if (playPromise !== undefined) {
        playPromise.catch(err => {
            console.log('Audio playback failed:', err);
            // Fallback UI
            const fallbackLink = node.episode_url ? `<br><br><strong>Audio unavailable.</strong> <a href="${node.episode_url}" target="_blank" style="color: #4CAF50;">Listen on external source ↗</a>` : '<br><br><strong>Audio unavailable.</strong>';
            textEl.innerHTML = node.text + fallbackLink;
        });
    }

    setupAudioSync(node);
}

function setupAudioSync(startNode) {
    const audio = document.getElementById('audio-element');
    audio.removeEventListener('timeupdate', handleAudioSync);
    audio.addEventListener('timeupdate', handleAudioSync);

    function handleAudioSync() {
        if (clickProtectionTimeout) return;
        const currentTime = audio.currentTime;
        const episodeChunks = graphData.points
            .filter(n => n.episode_id === startNode.episode_id)
            .sort((a, b) => a.timestamp - b.timestamp);

        let currentChunk = null;
        for (let i = 0; i < episodeChunks.length; i++) {
            if (episodeChunks[i].timestamp <= currentTime) {
                currentChunk = episodeChunks[i];
            } else {
                break;
            }
        }

        if (currentChunk && currentChunk.id !== activeNode?.id) {
            activeNode = currentChunk;
            graph
                .nodeColor(graph.nodeColor())
                .nodeVal(graph.nodeVal());

            if (cameraFollowEnabled) {
                const distance = 200;
                const distRatio = 1 + distance/Math.hypot(currentChunk.x, currentChunk.y, currentChunk.z);
                controls.target.set(currentChunk.x, currentChunk.y, currentChunk.z);
                camera.position.set(currentChunk.x * distRatio, currentChunk.y * distRatio, currentChunk.z * distRatio);
            }
            document.getElementById('player-text').textContent = currentChunk.text;
        }
    }
}

function setupControls() {
    const episodeSelect = document.getElementById('episode-selector');
    const sortedEpisodes = [...graphData.episodes].sort((a, b) => {
        const matchA = a.title.match(/(?:Episode\s+|Ep\s+)?(\d+)/i);
        const matchB = b.title.match(/(?:Episode\s+|Ep\s+)?(\d+)/i);
        if (matchA && matchB) {
            return parseInt(matchA[1]) - parseInt(matchB[1]);
        }
        return a.title.localeCompare(b.title);
    });

    sortedEpisodes.forEach(ep => {
        const option = document.createElement('option');
        option.value = ep.episode_id;
        option.textContent = ep.title;
        episodeSelect.appendChild(option);
    });

    episodeSelect.addEventListener('change', (e) => {
        const newEpisode = e.target.value || null;
        if (newEpisode) {
            currentEpisode = newEpisode;
            const episodeChunks = graphData.points
                .filter(n => n.episode_id === currentEpisode)
                .sort((a, b) => a.timestamp - b.timestamp);
            if (episodeChunks.length > 0) {
                handleNodeClick(episodeChunks[0]);
            }
        } else {
            const player = document.getElementById('audio-player');
            const audio = document.getElementById('audio-element');
            player.classList.remove('active');
            audio.pause();
            activeNode = null;
            currentEpisode = null;
            currentCluster = null;
            document.getElementById('cluster-selector').value = '';
            clearSelection();
        }
    });

    updateClusterSelector();

    document.getElementById('cluster-selector').addEventListener('change', (e) => {
        currentCluster = e.target.value === '' ? null : parseInt(e.target.value);
        graph.nodeColor(graph.nodeColor()).linkOpacity(graph.linkOpacity());
    });

    document.getElementById('cluster-level').addEventListener('change', (e) => {
        currentClusterLevel = e.target.value;
        clusterColors = {};
        if (graphData.clusters_by_level && graphData.clusters_by_level[currentClusterLevel]) {
            graphData.clusters_by_level[currentClusterLevel].forEach(cluster => {
                clusterColors[cluster.id] = cluster.color;
            });
        }
        updateClusterSelector();
        graph.nodeColor(graph.nodeColor()).graphData(getFilteredData());
    });

    document.getElementById('playback-speed').addEventListener('change', (e) => {
        document.getElementById('audio-element').playbackRate = parseFloat(e.target.value);
    });

    document.getElementById('player-close').addEventListener('click', () => {
        document.getElementById('audio-player').classList.remove('active');
        document.getElementById('audio-element').pause();
        activeNode = null;
        graph.nodeColor(graph.nodeColor());
    });
}

function updateClusterSelector() {
    const clusterSelect = document.getElementById('cluster-selector');
    const clusters = graphData.clusters_by_level[currentClusterLevel];
    clusterSelect.innerHTML = '<option value="">All Topics</option>';
    clusters.forEach(cluster => {
        const option = document.createElement('option');
        option.value = cluster.cluster_id;
        option.textContent = `${cluster.name} (${cluster.count})`;
        clusterSelect.appendChild(option);
    });
    currentCluster = null;
}
