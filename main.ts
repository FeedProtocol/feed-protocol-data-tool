import { Connection, ParsedTransactionWithMeta, PublicKey, SignaturesForAddressOptions } from '@solana/web3.js';
import { createObjectCsvWriter } from 'csv-writer';
import { sleep } from './utils';

async function getAllSignatures(address: string, connection: Connection) {
    let allSignatures: Array<{ signature: string }> = [];
    let continueFetching = true;
    let lastSignature: string | null = null;

    while (continueFetching) {
        const signatures = await getPaginatedSignatures(address, 10, lastSignature, connection);

        if (signatures.length === 0) {
            continueFetching = false;
        } else {
            allSignatures = allSignatures.concat(signatures);
            lastSignature = signatures[signatures.length - 1].signature;
        }
    }

    return allSignatures;
}

async function getPaginatedSignatures(address: string, limit: number = 10, before: string | null = null, connection: Connection) {
    let query: SignaturesForAddressOptions = { limit: limit };

    if (before) {
        query.before = before;
    }

    const signatures = await connection.getSignaturesForAddress(new PublicKey(address), query);
    return signatures;
}

async function getTransactionDetails(signatures: Array<{ signature: string; }>, connection: Connection): Promise<(ParsedTransactionWithMeta | null)[]> {
    const transactions: ParsedTransactionWithMeta[] = [];

    for (let index = 0; index < signatures.length; index++) {
        const signature = signatures[index];
        const parsedTransaction = await connection.getParsedTransaction(signature.signature, {
            maxSupportedTransactionVersion: 0,
        });
        if (parsedTransaction) {
            transactions.push(parsedTransaction);
        }

        sleep(1000);
    }
    return transactions;
}

async function fetchData(programAddress: string, rpcUrl: string, name: string) {
    try {
        sleep(1000);
        const connection = new Connection(rpcUrl, "confirmed");
        const allSignatures = await getAllSignatures(programAddress, connection);
        if (allSignatures && allSignatures.length > 0) {
            sleep(1000);
            const transactionDetails = await getTransactionDetails(allSignatures, connection);
            if (transactionDetails && transactionDetails.length > 0) {
                sleep(1000);

                const csvWriter = createObjectCsvWriter({
                    path: name + '.csv',
                    header: [
                        { id: 'signature', title: 'Signature' },
                        { id: 'caller', title: 'Caller' },
                        { id: 'slot', title: 'Slot' },
                        { id: 'blockTime', title: 'Block Time' },
                        { id: 'metadata', title: 'Metadata' },
                        { id: 'rawData', title: 'Raw Data' },
                    ],
                });

                const validTransactions = transactionDetails.filter((tx) => tx !== null);

                if (validTransactions && validTransactions.length > 0) {
                    const records = validTransactions.map((tx) => ({
                        signature: tx.transaction.signatures[0],
                        caller: tx.transaction.message.accountKeys[0].pubkey.toBase58(),
                        slot: tx.slot,
                        blockTime: tx.blockTime ? new Date(tx.blockTime * 1000).toUTCString() : 'N/A',
                        metadata: JSON.stringify(tx.meta),
                        rawData: JSON.stringify(tx.transaction),
                    }));

                    await csvWriter.writeRecords(records);
                }
            }
        }
    } catch (error) {
        console.log(name + " " + error);
    }
}

async function main() {
    await fetchData("9uSwASSU59XvUS8d1UeU8EwrEzMGFdXZvQ4JSEAfcS7k", "https://api.devnet.solana.com", "solana-devnet-old");
    await fetchData("9uSwASSU59XvUS8d1UeU8EwrEzMGFdXZvQ4JSEAfcS7k", "https://api.testnet.solana.com", "solana-testnet-old");
    await fetchData("9uSwASSU59XvUS8d1UeU8EwrEzMGFdXZvQ4JSEAfcS7k", "https://api.mainnet-beta.solana.com", "solana-mainnet-old");

    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://api.devnet.solana.com", "solana-devnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://api.testnet.solana.com", "solana-testnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://api.mainnet-beta.solana.com", "solana-mainnet");

    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://staging-rpc.dev2.eclipsenetwork.xyz", "eclipse-devnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://testnet.dev2.eclipsenetwork.xyz", "eclipse-testnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://mainnetbeta-rpc.eclipse.xyz", "eclipse-mainnet");

    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://devnet.sonic.game", "sonic-devnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://api.testnet.sonic.game", "sonic-testnet");

    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://rpc.devnet.soo.network/rpc", "soon-devnet");
    await fetchData("FEED1qspts3SRuoEyG29NMNpsTKX8yG9NGMinNC4GeYB", "https://rpc.testnet.soo.network/rpc", "soon-testnet");
}

main().catch(err => {
    console.error(err);
});