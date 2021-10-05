import url from "url";
import querystring from "querystring";
import {createIndex} from "../services/elasticService";
import {spawnNewNodeIfThresholdExceeded} from "../services/gkeService";

export async function trackGKE(req, res) {
    setInterval(async() => {
        await spawnNewNodeIfThresholdExceeded();
    }, 1000 * 60 * 3);
    return res.success()
}
