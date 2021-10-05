import elasticsearch from 'elasticsearch';
import crypto from "crypto";

export const client = new elasticsearch.Client({ hosts: ['http://103.195.5.137:9200'] });

export async function createIndex(index) {
    let indexExists = await checkIfIndexExists(index);
    if(!indexExists) {
        await client.indices.create({
            index
        })
    }
}

export async function addDocumentWithId(index, id, body) {
    const hashedId = crypto.createHash('md5').update(id).digest('hex');
    await client.index({
        id: hashedId,
        index,
        body
    });
}

export async function addDocumentWithoutId(index, body) {
    await client.index({
        index,
        body
    });
}

export async function getDocumentInIndexById(index, id) {
    const hashedId = crypto.createHash('md5').update(id).digest('hex');
    const val = await client.exists({
        id: hashedId,
        index
    });
    return val ? await client.get({
        id: hashedId,
        index
    }) : null;
}

async function checkIfIndexExists(index) {
    return await client.indices.exists({
        index
    });
}