"""Static ABI subset. Unknown events are retained, never guessed."""
from Crypto.Hash import keccak


def topic(signature):
    return '0x' + keccak.new(digest_bits=256, data=signature.encode()).hexdigest()


def event(name, fields):
    fields = [tuple(f.split()) for f in fields.split(',')]
    return {'name': name, 'fields': fields,
            'topic': topic(name + '(' + ','.join(f[0] for f in fields) + ')')}


SPECS = {
    'pons_v1': [event('TokenLaunched', 'address token indexed,address deployer indexed,address dexFactory indexed,address pairToken,address pool,uint256 dexId,uint256 launchConfigId,uint256 positionId,uint256 restrictionsEndBlock,uint256 initialBuyAmount')],
    'pons_v2': [event('TokenLaunched', 'address token indexed,address curve indexed,address deployer indexed,address pairToken,uint256 launchConfigId,uint256 graduationThreshold'), event('LaunchSwept', 'address token indexed,uint256 quoteOut,uint256 tokenOut')],
    'long': [event('Create', 'address asset,address numeraire indexed,address initializer,address poolOrHook'), event('Migrate', 'address asset indexed,address pool indexed')],
    'curve': [event('CurveBuy', 'address buyer indexed,address recipient indexed,uint256 quoteIn,uint256 tokensOut,uint256 fee,uint256 tax'), event('CurveSell', 'address seller indexed,address recipient indexed,uint256 tokensIn,uint256 quoteOut,uint256 fee,uint256 tax'), event('CurveCompleted', 'address recipient,uint256 quoteOut,uint256 tokenOut'), event('CurveBuyRefunded', 'address buyer indexed,uint256 refund')],
    'v3_pool': [event('Swap', 'address sender indexed,address recipient indexed,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick')],
    'v4': [event('Initialize', 'bytes32 id indexed,address currency0 indexed,address currency1 indexed,uint24 fee,int24 tickSpacing,address hooks,uint160 sqrtPriceX96,int24 tick'), event('ModifyLiquidity', 'bytes32 id indexed,address sender indexed,int24 tickLower,int24 tickUpper,int256 liquidityDelta,bytes32 salt'), event('Swap', 'bytes32 id indexed,address sender indexed,int128 amount0,int128 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick,uint24 fee')],
    'v2_factory': [event('PairCreated', 'address token0 indexed,address token1 indexed,address pair,uint256 pairCount')],
    'v3_factory': [event('PoolCreated', 'address token0 indexed,address token1 indexed,uint24 fee indexed,int24 tickSpacing,address pool')],
    'erc6551_registry': [event('ERC6551AccountCreated', 'address account,address implementation indexed,bytes32 salt,uint256 chainId,address tokenContract indexed,uint256 tokenId indexed')],
}


def decode(kind, log):
    specs = {s['topic']: s for s in SPECS[kind]}
    spec = specs.get(log['topics'][0].lower()) if log.get('topics') else None
    if not spec:
        return 'Unknown', {}
    topics = log['topics'][1:]
    raw = bytes.fromhex(log['data'].removeprefix('0x'))
    if len(topics) != sum(len(f) == 3 for f in spec['fields']):
        raise ValueError('indexed field count mismatch')
    if len(raw) != 32 * sum(len(f) == 2 for f in spec['fields']):
        raise ValueError('data length mismatch')
    ti = di = 0
    values = {}
    for f in spec['fields']:
        typ, name = f[:2]
        if len(f) == 3:
            word = bytes.fromhex(topics[ti].removeprefix('0x')); ti += 1
        else:
            word = raw[di:di + 32]; di += 32
        if len(word) != 32:
            raise ValueError('invalid ABI word')
        if typ == 'address':
            if any(word[:12]): raise ValueError('invalid address padding')
            value = '0x' + word[-20:].hex()
        elif typ == 'bytes32':
            value = '0x' + word.hex()
        else:
            signed = typ.startswith('int')
            number = int.from_bytes(word, 'big', signed=signed)
            bits = int(typ[3:] if signed else typ[4:])
            valid = -(2 ** (bits - 1)) <= number < 2 ** (bits - 1) if signed else 0 <= number < 2 ** bits
            if not valid:
                raise ValueError('integer outside ABI range')
            value = str(number)  # JSON-safe precision for all downstream consumers
        values[name] = value
    return spec['name'], values
