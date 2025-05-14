from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from sugar.chains import AsyncBaseChain
import redis
import json
import asyncio
import os

from sugar.chains import AsyncBaseChain, AsyncBscChain

# Connector tokens
connector_tokens = {
    56: {
        "0x558225E240D8C73dF754C48b330DE5f281ee99B9", # GOB v2
        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", # WBNB
        "0x205f59C72385C82b2328FC1c7776640C8d10f836", # tGOB v1
        "0x767Dc7981a5d58539814110dEA8dd88857164fa1", # tBCH
        "0xa44319D6232afEAa21A38b040Ca095110ad76d38", # tUSDT
        "0x53b6a051dD3193d4F80c1E66c56316af180755F6", # tUSDC
        "0x4fAd9b2458634B1E5D679732ca3f6C203e565B13"  # tETH
    },
    8453: {
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
        "0x940181a94A35A4569E4529A3CDfB74e38FD98631",  # AERO
        "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "0x4621b7a9c75199271f773ebd9a499dbd165c3191",
        "0x4200000000000000000000000000000000000006",
        "0xb79dd08ea68a908a97220c76d19a6aa9cbde4376",
        "0xf7a0dd3317535ec4f4d29adf9d620b3d8d5d5069",
        "0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4",
        "0xcb327b99ff831bf8223cced12b1338ff3aa322ff",
        "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",
        "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452",
        "0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42",
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
        "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
    }
}

# Initialize FastAPI and Redis
app = FastAPI()
redis_client = redis.Redis(host=os.environ.get("REDIS_HOST") or 'localhost', port=6379, db=0, decode_responses=True)
scheduler = BackgroundScheduler()
scheduler.start()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fetch pools and cache them every 10 minutes
async def cache_pools(chainId: int):
    asyncChain = AsyncBaseChain if chainId == 8453 else AsyncBscChain

    async with asyncChain() as chain:
        pools = await chain.get_pools()
        pools_data = []
        for pool in pools:
            pools_data.append({
                "token0": {"token_address": pool.token0.token_address},
                "token1": {"token_address": pool.token1.token_address},
                "reserve0": {"amount": str(pool.reserve0.amount if pool.reserve0 else 0)},
                "reserve1": {"amount": str(pool.reserve1.amount if pool.reserve1 else 0)},
                "pool_fee": pool.pool_fee,
                "is_cl": pool.is_cl,
                "is_stable": pool.is_stable,
                "factory": pool.factory
            })
        redis_client.set("pools" + str(chainId), json.dumps(pools_data), ex=660)  # Cache for 11 minutes

# Initial pool cache when server starts
@app.on_event("startup")
async def startup_event():
    await asyncio.gather(*[
        cache_pools(56), # BSC
        cache_pools(8453), # BASE
    ])

# Run the cache_pools function periodically
@scheduler.scheduled_job('interval', minutes=10)
def scheduled_job():
    asyncio.run(cache_pools(56)) # BSC
    asyncio.run(cache_pools(8453)) # BASE

# Fetch pools from Redis
async def get_pools_from_cache(chainId: int):
    cached_pools = redis_client.get("pools"+str(chainId))
    if cached_pools:
        return json.loads(cached_pools)
    else:
        await cache_pools(chainId)  # Fetch and cache if not found
        return json.loads(redis_client.get("pools")+str(chainId))


async def fetch_all_and_best_routes(chainId: int, from_token, to_token):
    from_token = from_token.lower()
    to_token = to_token.lower()

    pools = await get_pools_from_cache(chainId)
    all_routes = []
    best_route = None
    best_score = float("-inf")

    # Create pool map for fast lookup
    pool_map = {}
    for pool in pools:
        t0 = pool['token0']['token_address'].lower()
        t1 = pool['token1']['token_address'].lower()
        pool_map.setdefault(t0, []).append(pool)
        pool_map.setdefault(t1, []).append(pool)

    # Step 1: Direct Routes
    for pool in pool_map.get(from_token, []):
        t0, t1 = pool['token0']['token_address'].lower(), pool['token1']['token_address'].lower()
        if t0 == to_token or t1 == to_token:
            liquidity = float(pool['reserve0']['amount']) + float(pool['reserve1']['amount'])
            fee = pool['pool_fee']
            score = liquidity / (fee + 1e-6)

            route = ([{"from": from_token, "fee": fee, "to": to_token, "factory": pool['factory']}] if pool['is_cl']
                     else [{"from": from_token, "to": to_token, "stable": pool['is_stable'], "factory": pool['factory']}])

            all_routes.append({"route": route})
            if score > best_score:
                best_score = score
                best_route = route

    # Step 2: One-Hop Routes (Using Connector Tokens)
    for connector in connector_tokens[chainId]:
        connector = connector.lower()
        if connector == from_token or connector == to_token:
            continue

        pool1 = next((pool for pool in pool_map.get(from_token, [])
                      if pool['token0']['token_address'].lower() == connector or pool['token1']['token_address'].lower() == connector), None)

        pool2 = next((pool for pool in pool_map.get(connector, [])
                      if pool['token0']['token_address'].lower() == to_token or pool['token1']['token_address'].lower() == to_token), None)

        if pool1 and pool2:
            liquidity1 = float(pool1['reserve0']['amount']) + float(pool1['reserve1']['amount'])
            liquidity2 = float(pool2['reserve0']['amount']) + float(pool2['reserve1']['amount'])
            total_liquidity = liquidity1 + liquidity2
            total_fee = pool1['pool_fee'] + pool2['pool_fee']
            score = total_liquidity / (total_fee + 1e-6)

            route = []
            if pool1['is_cl']:
                route.append({"from": from_token, "fee": pool1['pool_fee'], "to": connector, "factory": pool1['factory']})
            else:
                route.append({"from": from_token, "to": connector, "stable": pool1['is_stable'], "factory": pool1['factory']})

            if pool2['is_cl']:
                route.append({"from": connector, "fee": pool2['pool_fee'], "to": to_token, "factory": pool2['factory']})
            else:
                route.append({"from": connector, "to": to_token, "stable": pool2['is_stable'], "factory": pool2['factory']})

            all_routes.append({"route": route})
            if score > best_score:
                best_score = score
                best_route = route

    return all_routes, best_route



# Quote route
@app.get("/quote")
async def quote(token0: str, token1: str, chainId: int, amount: int):
    try:
        if chainId == 84532:
            return {"data": [{ "from": token0, "stable": False, "to": token1, "factory": "0x5F47613A76C1c01BcE11b3D398de16E38c3d4DCb" }], "command_type": "V2_SWAP_EXACT_IN"}
        else:
            all_routes, best_route = await fetch_all_and_best_routes(chainId, token0.lower(), token1.lower())
            command_type = "V2_SWAP_EXACT_IN" if not best_route or not isinstance(best_route, list) or not best_route[0].get("fee") else "V3_SWAP_EXACT_IN"
            return {"data": best_route, "command_type": command_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

