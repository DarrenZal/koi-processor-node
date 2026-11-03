// Planetary Regeneration Podcast 3D Map
// Based on an earlier podcast map implementation

let graphData = null;
let graph = null;
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
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function getMutedColor(hexColor) {
    const r = parseInt(hexColor.slice(1, 3), 16);
    const g = parseInt(hexColor.slice(3, 5), 16);
    const b = parseInt(hexColor.slice(5, 7), 16);
    const bgR = 10, bgG = 14, bgB = 26;
    const mutedR = Math.round(r * 0.2 + bgR * 0.8);
    const mutedG = Math.round(g * 0.2 + bgG * 0.8);
    const mutedB = Math.round(b * 0.2 + bgB * 0.8);
    return `#${mutedR.toString(16).padStart(2, '0')}${mutedG.toString(16).padStart(2, '0')}${mutedB.toString(16).padStart(2, '0')}`;
}

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    try {
        const response = await fetch('/static/podcast/podcast_map_3d.json?v=' + Date.now());
        graphData = await response.json();

        console.log('Loaded data:', {
            points: graphData.points.length,
            episodes: graphData.episodes.length,
            links: graphData.links.length
        });

        // Build cluster colors map
        if (graphData.clusters_by_level && graphData.clusters_by_level[currentClusterLevel]) {
            graphData.clusters_by_level[currentClusterLevel].forEach(cluster => {
                clusterColors[cluster.id] = cluster.color;
            });
        }

        initializeGraph();
        setupControls();
        document.getElementById('loading').style.display = 'none';
    } catch (error) {
        console.error('Error loading data:', error);
        document.getElementById('loading').textContent = 'Error loading data';
    }
});

function initializeGraph() {
    const container = document.getElementById('graph-container');
    const clusterKey = `cluster_${currentClusterLevel}`;
    const clusterNameKey = `cluster_${currentClusterLevel}_name`;

    graph = window.graph = ForceGraph3D()(container)
        .graphData(getFilteredData())
        .nodeId('id')
        .nodeLabel(node => `<strong>${node.episode_title}</strong><br>Topic: ${node[clusterNameKey]}<br>${node.text.substring(0, 100)}...`)
        .nodeColor(node => {
            if (activeNode && node.id === activeNode.id) {
                return '#00ff00';
            }
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
                // When episode selected: emphasize episode nodes (much larger)
                return node.episode_id === currentEpisode ? 12 : 1;
            }
            if (currentCluster !== null) {
                // When cluster selected: emphasize cluster nodes (larger)
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
                // Show links brighter for selected cluster
                const key = `cluster_${currentClusterLevel}`;
                const sourceNode = graphData.points.find(n => n.id === link.source);
                const targetNode = graphData.points.find(n => n.id === link.target);
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
                const sourceNode = graphData.points.find(n => n.id === link.source);
                const targetNode = graphData.points.find(n => n.id === link.target);
                if (sourceNode && targetNode) {
                    const bothInCluster = sourceNode[key] === currentCluster && targetNode[key] === currentCluster;
                    return bothInCluster ? 2.0 : 0.3;
                }
            }
            return 0.8;
        })
        .backgroundColor('#0a0e1a')
        .showNavInfo(false)
        .onNodeClick(handleNodeClick)
        .onNodeHover(node => {
            document.body.style.cursor = node ? 'pointer' : 'default';
        })
        .enableNodeDrag(false)
        .d3Force('center', null)
        .d3Force('charge', null)
        .d3Force('link', d3.forceLink().distance(20).strength(0.1));

    graph.cameraPosition({ z: 2500 });

    // Fix node opacity - 3D-Force-Graph bug where nodeOpacity doesn't work
    setTimeout(() => {
        const scene = graph.scene();
        const group = scene.children.find(c => c.type === 'Group');
        if (group) {
            group.children.forEach(child => {
                if (child.type === 'Mesh' && child.material) {
                    child.visible = true;
                    child.material.opacity = 0.8;
                    child.material.transparent = true;
                    child.material.needsUpdate = true;
                }
            });
            console.log(`Fixed opacity for ${group.children.length} nodes`);
        }
    }, 500);

    startAutoRotation();

    // Detect user interaction to disable auto-rotation and camera follow
    function disableAutoFeatures() {
        autoRotationEnabled = false;
        cameraFollowEnabled = false;

        // Re-enable camera follow after 5 seconds of no interaction
        if (userInteractionTimeout) clearTimeout(userInteractionTimeout);
        userInteractionTimeout = setTimeout(() => {
            cameraFollowEnabled = true;
        }, 5000);
    }

    container.addEventListener('mousedown', disableAutoFeatures);
    container.addEventListener('wheel', disableAutoFeatures);

    // Touch event handlers for mobile gesture detection
    container.addEventListener('touchstart', (e) => {
        touchStartTime = Date.now();
        touchStartPos = { x: e.touches[0].clientX, y: e.touches[0].clientY };
        isTouchGesture = e.touches.length > 1; // Multi-touch = gesture
        disableAutoFeatures();
    });

    container.addEventListener('touchmove', (e) => {
        if (e.touches.length > 1) {
            isTouchGesture = true; // Pinch zoom
        } else if (e.touches.length === 1) {
            const dx = Math.abs(e.touches[0].clientX - touchStartPos.x);
            const dy = Math.abs(e.touches[0].clientY - touchStartPos.y);
            if (dx > 10 || dy > 10) {
                isTouchGesture = true; // Drag/pan
            }
        }
    });

    container.addEventListener('touchend', () => {
        // Reset gesture flag after a short delay
        setTimeout(() => {
            isTouchGesture = false;
        }, 100);
    });
}

// Links are now handled by 3D-Force-Graph's built-in rendering with dynamic opacity/width functions

function addClusterBoundaries() {
    const scene = graph.scene();
    const sampleMesh = scene.children.find(child => child.type === 'Mesh' && child.geometry);
    if (!sampleMesh) {
        console.warn('No mesh nodes found, skipping cluster boundaries');
        return;
    }

    const clusterKey = `cluster_${currentClusterLevel}`;
    const nodesByCluster = {};
    const data = getFilteredData();

    data.nodes.forEach(node => {
        const clusterId = node[clusterKey];
        if (!nodesByCluster[clusterId]) {
            nodesByCluster[clusterId] = [];
        }
        nodesByCluster[clusterId].push(node);
    });

    const SphereGeometryConstructor = sampleMesh.geometry.constructor;
    const MeshConstructor = sampleMesh.constructor;
    const MaterialConstructor = sampleMesh.material.constructor;

    Object.keys(nodesByCluster).forEach(clusterId => {
        const nodes = nodesByCluster[clusterId];
        if (nodes.length < 4) return;

        const bounds = calculateClusterBounds(nodes);
        const color = clusterColors[clusterId] || '#888888';
        const mutedColor = getMutedColor(color);

        const geometry = new SphereGeometryConstructor(1, 16, 12);
        geometry.scale(bounds.scaleX, bounds.scaleY, bounds.scaleZ);

        const material = new MaterialConstructor({
            color: mutedColor,
            transparent: true,
            opacity: 0.12,
            side: 2,
            depthWrite: false
        });

        const mesh = new MeshConstructor(geometry, material);
        mesh.position.set(bounds.centerX, bounds.centerY, bounds.centerZ);
        scene.add(mesh);
    });

    console.log(`Added ${Object.keys(nodesByCluster).length} cluster boundaries`);
}

function calculateClusterBounds(nodes) {
    let minX = Infinity, maxX = -Infinity;
    let minY = Infinity, maxY = -Infinity;
    let minZ = Infinity, maxZ = -Infinity;
    let sumX = 0, sumY = 0, sumZ = 0;

    nodes.forEach(node => {
        minX = Math.min(minX, node.x);
        maxX = Math.max(maxX, node.x);
        minY = Math.min(minY, node.y);
        maxY = Math.max(maxY, node.y);
        minZ = Math.min(minZ, node.z);
        maxZ = Math.max(maxZ, node.z);
        sumX += node.x;
        sumY += node.y;
        sumZ += node.z;
    });

    const centerX = sumX / nodes.length;
    const centerY = sumY / nodes.length;
    const centerZ = sumZ / nodes.length;
    const scaleX = ((maxX - minX) / 2) * 1.3 || 10;
    const scaleY = ((maxY - minY) / 2) * 1.3 || 10;
    const scaleZ = ((maxZ - minZ) / 2) * 1.3 || 10;

    return { centerX, centerY, centerZ, scaleX, scaleY, scaleZ };
}

function getFilteredData() {
    if (!graphData) return { nodes: [], links: [] };

    // Always show all nodes - never filter them out
    const nodes = graphData.points;
    const links = graphData.links;

    console.log('getFilteredData:', {
        totalNodes: nodes.length,
        totalLinks: links.length,
        currentCluster: currentCluster,
        currentEpisode: currentEpisode
    });

    return { nodes, links };
}

function handleNodeClick(node) {
    // Ignore clicks during touch gestures (pinch/zoom/pan)
    if (isTouchGesture) {
        console.log('Ignoring click during touch gesture');
        return;
    }

    if (clickProtectionTimeout) {
        clearTimeout(clickProtectionTimeout);
    }
    clickProtectionTimeout = setTimeout(() => {
        clickProtectionTimeout = null;
    }, 2000);

    console.log('Clicked node:', node);
    activeNode = node;
    currentEpisode = node.episode_id;

    // Highlight the episode by updating node and link appearance
    highlightEpisode(node.episode_id);

    const distance = 200;
    const distRatio = 1 + distance/Math.hypot(node.x, node.y, node.z);
    graph.cameraPosition(
        { x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio },
        node,
        1000
    );

    playAudio(node);
}

function highlightEpisode(episodeId) {
    currentEpisode = episodeId;

    // Update node appearance - make selected episode nodes larger and brighter
    graph.nodeColor(node => {
        if (activeNode && node.id === activeNode.id) {
            return '#00ff00';
        }
        const clusterKey = `cluster_${currentClusterLevel}`;
        const nodeColor = clusterColors[node[clusterKey]] || '#666';
        if (node.episode_id === episodeId) {
            return nodeColor;
        }
        return hexToRgba(nodeColor, 0.2);
    });

    graph.nodeVal(node => {
        if (activeNode && node.id === activeNode.id) return 20;
        return node.episode_id === episodeId ? 12 : 1;
    });

    // Update link appearance - make selected episode links thicker and brighter
    graph.linkOpacity(link => {
        return link.episode_id === episodeId ? 0.8 : 0.05;
    });

    graph.linkWidth(link => {
        return link.episode_id === episodeId ? 3.0 : 0.3;
    });
}

function clearSelection() {
    currentEpisode = null;
    activeNode = null;

    // Reset node colors and sizes to default using dynamic functions
    const clusterKey = `cluster_${currentClusterLevel}`;
    graph.nodeColor(node => {
        if (activeNode && node.id === activeNode.id) {
            return '#00ff00';
        }
        const nodeColor = clusterColors[node[clusterKey]] || '#666';
        if (currentEpisode) {
            return node.episode_id === currentEpisode ? nodeColor : hexToRgba(nodeColor, 0.2);
        }
        if (currentCluster !== null) {
            return node[clusterKey] === currentCluster ? nodeColor : hexToRgba(nodeColor, 0.2);
        }
        return nodeColor;
    });

    graph.nodeVal(node => {
        if (activeNode && node.id === activeNode.id) return 20;
        if (currentEpisode) {
            return node.episode_id === currentEpisode ? 12 : 1;
        }
        if (currentCluster !== null) {
            return node[clusterKey] === currentCluster ? 8 : 1;
        }
        return 2;
    });

    // Reset link appearance to default using dynamic functions
    graph.linkOpacity(link => {
        if (currentEpisode) {
            return link.episode_id === currentEpisode ? 0.8 : 0.05;
        }
        if (currentCluster !== null) {
            const key = `cluster_${currentClusterLevel}`;
            const sourceNode = graphData.points.find(n => n.id === link.source);
            const targetNode = graphData.points.find(n => n.id === link.target);
            if (sourceNode && targetNode) {
                const bothInCluster = sourceNode[key] === currentCluster && targetNode[key] === currentCluster;
                return bothInCluster ? 0.6 : 0.05;
            }
        }
        return link.opacity || 0.3;
    });

    graph.linkWidth(link => {
        if (currentEpisode) {
            return link.episode_id === currentEpisode ? 3.0 : 0.3;
        }
        if (currentCluster !== null) {
            const key = `cluster_${currentClusterLevel}`;
            const sourceNode = graphData.points.find(n => n.id === link.source);
            const targetNode = graphData.points.find(n => n.id === link.target);
            if (sourceNode && targetNode) {
                const bothInCluster = sourceNode[key] === currentCluster && targetNode[key] === currentCluster;
                return bothInCluster ? 2.0 : 0.3;
            }
        }
        return 0.8;
    });
}

function playAudio(node) {
    const player = document.getElementById('audio-player');
    const audio = document.getElementById('audio-element');
    const titleEl = document.getElementById('player-episode-title');
    const textEl = document.getElementById('player-text');

    // Stop any currently playing audio first
    if (!audio.paused) {
        audio.pause();
    }
    audio.currentTime = 0;

    // Show player and update content
    player.classList.add('active');
    titleEl.textContent = node.episode_title;
    textEl.textContent = node.text;

    // Set new audio source and play
    const audioUrl = node.audio_url || node.episode_url;

    // If changing to a different source, update src
    if (audio.src !== audioUrl) {
        audio.src = audioUrl;
    }

    audio.currentTime = node.timestamp;
    audio.play().catch(err => {
        console.log('Audio playback failed:', err);
        textEl.textContent = `${node.text}\n\n⚠️ Audio playback unavailable. Please visit: ${node.episode_url}`;
    });

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
            graph.graphData(getFilteredData());

            // Only move camera if user hasn't interacted recently
            if (cameraFollowEnabled) {
                const distance = 200;
                const distRatio = 1 + distance/Math.hypot(currentChunk.x, currentChunk.y, currentChunk.z);
                graph.cameraPosition(
                    { x: currentChunk.x * distRatio, y: currentChunk.y * distRatio, z: currentChunk.z * distRatio },
                    currentChunk,
                    500
                );
            }

            document.getElementById('player-text').textContent = currentChunk.text;
        }
    }
}

function setupControls() {
    const episodeSelect = document.getElementById('episode-selector');

    // Sort episodes by extracting episode number from title
    const sortedEpisodes = [...graphData.episodes].sort((a, b) => {
        // Extract episode numbers from various formats:
        // "Planetary Regeneration Podcast Episode 1 ...", "Ep 41 ...", "046 ..."
        const matchA = a.title.match(/(?:Episode\s+|Ep\s+)?(\d+)/i);
        const matchB = b.title.match(/(?:Episode\s+|Ep\s+)?(\d+)/i);

        if (matchA && matchB) {
            return parseInt(matchA[1]) - parseInt(matchB[1]);
        }

        // Fallback to alphabetical sort if no episode number found
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
                // Click the first chunk which will highlight the episode
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

            // Reset cluster selector to "All Topics"
            document.getElementById('cluster-selector').value = '';

            // Reset to default view
            clearSelection();
            graph.graphData(getFilteredData());
        }
    });

    updateClusterSelector();

    document.getElementById('cluster-selector').addEventListener('change', (e) => {
        currentCluster = e.target.value === '' ? null : parseInt(e.target.value);
        graph.graphData(getFilteredData());
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

        // Update node colors to use new cluster level
        const clusterKey = `cluster_${currentClusterLevel}`;
        const clusterNameKey = `cluster_${currentClusterLevel}_name`;

        graph.nodeLabel(node => `<strong>${node.episode_title}</strong><br>Topic: ${node[clusterNameKey]}<br>${node.text.substring(0, 100)}...`);

        graph.nodeColor(node => {
            if (activeNode && node.id === activeNode.id) {
                return '#00ff00';
            }
            const nodeColor = clusterColors[node[clusterKey]] || '#666';
            if (currentEpisode) {
                return node.episode_id === currentEpisode ? nodeColor : hexToRgba(nodeColor, 0.2);
            }
            if (currentCluster !== null) {
                return node[clusterKey] === currentCluster ? nodeColor : hexToRgba(nodeColor, 0.2);
            }
            return nodeColor;
        });

        graph.graphData(getFilteredData());
    });

    document.getElementById('playback-speed').addEventListener('change', (e) => {
        const audio = document.getElementById('audio-element');
        audio.playbackRate = parseFloat(e.target.value);
    });

    document.getElementById('player-close').addEventListener('click', () => {
        const player = document.getElementById('audio-player');
        const audio = document.getElementById('audio-element');
        player.classList.remove('active');
        audio.pause();
        activeNode = null;
        graph.graphData(getFilteredData());
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

function startAutoRotation() {
    let angle = 0;
    const rotationSpeed = 0.05;

    function rotate() {
        if (autoRotationEnabled && graph) {
            angle += rotationSpeed;
            const distance = 2200;
            const x = distance * Math.sin(angle * Math.PI / 180);
            const z = distance * Math.cos(angle * Math.PI / 180);
            graph.cameraPosition({ x, y: 0, z }, { x: 0, y: 0, z: 0 }, 0);
        }
        requestAnimationFrame(rotate);
    }
    rotate();
}
