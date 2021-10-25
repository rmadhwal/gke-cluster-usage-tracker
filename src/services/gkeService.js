import _ from "lodash";
import monitoring from '@google-cloud/monitoring';
import container from '@google-cloud/container';
import {addDocumentWithoutId, createIndex} from "./elasticService";
import k8s from '@kubernetes/client-node';

const indexName = "gke-metric-data";
const availableNodeCpu = 3.5;
const availableNodeMemory = 13000;
const nodePoolName = "pool-1";

var pending_nodes = 0;
let realNodeUsage = {};

const time_to_provision_new_node = 60;

export async function spawnNewNodeIfThresholdExceeded(initialNodeUsage) {
    await createIndex(indexName)
    const projectId = 'qpefcs-course-project';

    const clusterManagerClient = new container.v1.ClusterManagerClient();

    const kc = new k8s.KubeConfig();
    kc.loadFromDefault();

    const k8sApi = kc.makeApiClient(k8s.CoreV1Api);
    const clusterRequest = {
        projectId: projectId,
        zone: 'us-central1-c',
    };

    while (true) {
        const pods = await k8sApi.listNamespacedPod('test');
        const [clusterInfoResponse] = await clusterManagerClient.listClusters(clusterRequest);
        const nodesUp = _.first(_.first(clusterInfoResponse.clusters).nodePools).initialNodeCount
        //phase: running, completed, pending
        const trainingJobs = _.filter(pods.body.items, function (item) {
            return item.metadata.labels["controller-name"] === "pytorch-operator";
        });

        const completedJobs = _.filter(trainingJobs, function (job) {
            return job.status.phase === "Succeeded";
        });
        const pendingJobs = _.filter(trainingJobs, function (job) {
            return job.status.phase === "Pending";
        });
        const runningJobs = _.filter(trainingJobs, function (job) {
            return job.status.phase === "Running";
        });

        const currentNodeUsage = Object.assign({}, initialNodeUsage);

        //Update known nodes
        _.forEach(runningJobs, function (job) {
            const jobNode = job.spec.nodeName
            const jobResources = _.first(job.spec.containers).resources.requests
            if (!jobNode in realNodeUsage)
                pending_nodes = pending_nodes - 1;
            currentNodeUsage[jobNode] = {
                "cpuUsed": (_.get(_.get(initialNodeUsage, "jobNode"), "cpuUsed") || 0) + parseInt(jobResources.cpu),
                "memoryUsed": (_.get(_.get(initialNodeUsage, "jobNode"), "memoryUsed") || 0) + (parseInt(jobResources.memory.replace(/\D/g, '')) * 1024)
            };
        });

        realNodeUsage = Object.assign({}, currentNodeUsage);

        //Populate nodes that are requested/running but have no jobs yet
        for (let i = 0; i < pending_nodes; i++) {
            currentNodeUsage[i.toString()] = {"cpuUsed": 0, "memoryUsed": 0}
        }

        const utilInfoToLog = _.map(_.keys(realNodeUsage), function mapToValue(node) {
            const nodeValue = realNodeUsage[node];
            return {
                "node": node,
                "metric": "utilization",
                "value": Math.min(nodeValue["cpuUsed"] / availableNodeCpu, nodeValue["memoryUsed"] / availableNodeMemory),
                "@timestamp": Math.floor(new Date().getTime() / 1000)
            };
        })

        //If we have no idea of the service time
        //Ensure we meet threshold capacity
        if (completedJobs.length === 0) {
            if (pendingJobs.length + runningJobs.length > 0) {

                const runningJobCpuSizes = _.map(runningJobs, function (job) {
                    return parseInt(_.first(job.spec.containers).resources.requests.cpu)
                });
                const pendingJobCpuSizes = _.map(pendingJobs, function (job) {
                    return parseInt(_.first(job.spec.containers).resources.requests.cpu)
                });
                const jobCpuSizes = _.concat(runningJobCpuSizes, pendingJobCpuSizes)

                const jobCpuSizeSum = jobCpuSizes.reduce((a, b) => a + b, 0);
                const jobCpuSizeAvg = (jobCpuSizeSum / jobCpuSizes.length);

                const runningJobMemorySizes = _.map(runningJobs, function (job) {
                    return parseInt(_.first(job.spec.containers).resources.requests.memory.replace(/\D/g, '')) * 1024;
                });
                const pendingJobMemorySizes = _.map(pendingJobs, function (job) {
                    return parseInt(_.first(job.spec.containers).resources.requests.memory.replace(/\D/g, '')) * 1024;
                });
                const jobMemorySizes = _.concat(runningJobMemorySizes, pendingJobMemorySizes);

                const jobMemorySizeSum = jobMemorySizes.reduce((a, b) => a + b, 0);
                const jobMemorySizeAvg = (jobMemorySizeSum / jobMemorySizes.length);

                const expectedNewJobs = runningJobs.length + pendingJobs.length;
                const jobsWeCanHandle = _.sum(_.map(currentNodeUsage, function (node) {
                    const jobsThatCanBeHandledCpuThreshold = Math.floor((availableNodeCpu - node["cpuUsed"]) / jobCpuSizeAvg)
                    const jobsThatCanBeHandledMemoryThreshold = Math.floor((availableNodeMemory - node["memoryUsed"]) / jobMemorySizeAvg)
                    return Math.min(jobsThatCanBeHandledCpuThreshold, jobsThatCanBeHandledMemoryThreshold)
                }));

                if (expectedNewJobs > jobsWeCanHandle) {
                    const jobsThatAreCurrentlyUnhandled = expectedNewJobs - jobsWeCanHandle;
                    const jobsPerNodeCpuThreshold = Math.floor(availableNodeCpu / jobCpuSizeAvg);
                    const jobsPerNodeMemoryThreshold = Math.floor(availableNodeMemory / jobMemorySizeAvg);
                    const jobsPerNode = Math.min(jobsPerNodeCpuThreshold, jobsPerNodeMemoryThreshold);
                    const newNodesRequired = Math.ceil(jobsThatAreCurrentlyUnhandled / jobsPerNode);
                    console.log(`Rescaling cluster to size ${newNodesRequired + nodesUp + pending_nodes}!`)
                    console.log(`Exepcted New Jobs ${expectedNewJobs}!`)
                    try {
                        await clusterManagerClient.setNodePoolSize({
                            nodePoolId: nodePoolName,
                            nodeCount: newNodesRequired + nodesUp + pending_nodes,
                            zone: 'us-central1-c',
                            projectId: "qpefcs-course-project",
                            clusterId: "flfk-cluster"
                        })
                        pending_nodes = newNodesRequired + pending_nodes;
                    } catch (e) {
                        console.log("Can't rescale yet")
                    }
                }
            }
        } else {
            const completedTimeDurations = _.map(completedJobs, function (job) {
                return _.first(job.status.containerStatuses).state.terminated.finishedAt - _.first(job.status.containerStatuses).state.terminated.startedAt
            });
            const allStartTimes = _.map(trainingJobs, function (job) {
                return job.metadata.creationTimestamp
            });
            const earliestJob = _.min(allStartTimes);
            const latestJob = _.max(allStartTimes);
            const serviceTimeAvg = (_.sum(completedTimeDurations) / completedTimeDurations.length) / 1000;
            const timeWindow = Math.min(serviceTimeAvg, (latestJob-earliestJob) / 1000);
            let maxArrivalRate = 0;
            var table = [];
            for (let j = 0; j < (latestJob - earliestJob) / (timeWindow * 1000); j++) {
                let filter = _.filter(allStartTimes, function (time) {
                    return new Date(earliestJob.getTime() + j * (timeWindow * 1000)) <= new Date(time) && new Date(time) <= new Date(earliestJob.getTime() + (j + 1) * (timeWindow * 1000));
                });
                table[j] = filter.length;
                maxArrivalRate = Math.max(filter.length / timeWindow, maxArrivalRate);
            }
            //calculate good estimate for service rate
            //and provision accordingly
        }

        _.forEach(utilInfoToLog, function indexToElasticsearch(utilInfo) {
            addDocumentWithoutId(indexName, utilInfo);
        })
    }
}
