import _ from "lodash";
import monitoring from "@google-cloud/monitoring";
import {spawnNewNodeIfThresholdExceeded} from "../services/gkeService";


export async function trackGKE(req, res) {
    const projectId = 'qpefcs-course-project';
    const cpuFilter = 'metric.type="kubernetes.io/node/cpu/allocatable_utilization"';
    const cpuScaling = 25;
    const memoryScaling = 102941;

    const client = new monitoring.MetricServiceClient();
    const cluster_scaling_request = {
        name: client.projectPath(projectId),
        filter: cpuFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 4,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [cpuTimeSeries] = await client.listTimeSeries(cluster_scaling_request);

    const memoryFilter = 'metric.type="kubernetes.io/node/memory/allocatable_utilization"';

    const memory_scaling_request = {
        name: client.projectPath(projectId),
        filter: memoryFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 4,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [memoryTimeSeries] = await client.listTimeSeries(memory_scaling_request);

    const gkeInitNodes = _.map(cpuTimeSeries, function extractName(row) {
        return row.resource.labels["node_name"]
    })

    const nodesResourceUsage = _.mapValues(_.keyBy(_.map(gkeInitNodes, function extractUtil(node) {
        const cpuUsed = _.first(_.find(cpuTimeSeries, function matchingNode(row) {
            return row.resource.labels["node_name"] === node;
        }).points).value.doubleValue;
        const memoryUsed = _.first(_.find(memoryTimeSeries, function matchingNode(row) {
            return row.resource.labels["node_name"] === node;
        }).points).value.doubleValue;
        return {"name": node, "util": {"cpuUsed": cpuUsed * cpuScaling, "memoryUsed": memoryUsed * memoryScaling}}
    }), "name"), "util");


    await spawnNewNodeIfThresholdExceeded(nodesResourceUsage);
}

await trackGKE();
