#!/usr/bin/env python3

import sys
import hashlib
import requests


# ============================================================
# PARTICIPANTS
# Numbering is fixed BEFORE the drawing.
# ============================================================

PARTICIPANTS = [
    "Alice",
    "Bob",
    "Charlie",
    "David",
    "Eve",
    "Frank",
    "Grace",
    "Henry",
    "Iris",
    "Jack",
    "Karen",
    "Larry",
    "Mary",
    "Nancy",
    "Oscar",
    "Patricia",
    "Quentin",
    "Rachel",
    "Steve",
    "Tom",
]


# ============================================================
# BITCOIN
# ============================================================

def get_block_hash(height):
    """
    Retrieve the Bitcoin mainnet block hash for a specific
    block height.
    """

    url = f"https://blockstream.info/api/block-height/{height}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    block_hash = response.text.strip()

    if len(block_hash) != 64:
        raise ValueError("Invalid Bitcoin block hash returned")

    return block_hash


# ============================================================
# DETERMINISTIC RANDOM NUMBER GENERATOR
# ============================================================

class BitcoinRandom:
    """
    Deterministic cryptographic random-number generator.

    The Bitcoin block hash is the seed.

    Anyone with the same block hash will generate exactly
    the same sequence of random numbers.
    """

    def __init__(self, block_hash):
        self.seed = bytes.fromhex(block_hash)
        self.counter = 0

    def random_bytes(self):
        """
        Generate deterministic 32-byte values using SHA-256.
        """

        data = (
            self.seed +
            self.counter.to_bytes(8, byteorder="big")
        )

        self.counter += 1

        return hashlib.sha256(data).digest()

    def randbelow(self, n):
        """
        Generate a uniform random integer from 0 through n-1.

        Rejection sampling is used to eliminate modulo bias.
        """

        if n <= 0:
            raise ValueError("n must be positive")

        max_value = 1 << 256
        limit = max_value - (max_value % n)

        while True:
            value = int.from_bytes(
                self.random_bytes(),
                byteorder="big"
            )

            if value < limit:
                return value % n


# ============================================================
# FISHER-YATES SHUFFLE
# ============================================================

def deterministic_shuffle(items, rng):
    """
    Cryptographically deterministic Fisher-Yates shuffle.
    """

    items = list(items)

    for i in range(len(items) - 1, 0, -1):
        j = rng.randbelow(i + 1)

        items[i], items[j] = items[j], items[i]

    return items


# ============================================================
# MAIN DRAW
# ============================================================

def main():

    if len(sys.argv) != 2:
        print("Usage:")
        print("  python3 draw.py BLOCK_HEIGHT")
        print()
        print("Example:")
        print("  python3 draw.py 925000")
        sys.exit(1)

    try:
        height = int(sys.argv[1])
    except ValueError:
        print("Block height must be an integer.")
        sys.exit(1)

    if height < 0:
        print("Block height must be non-negative.")
        sys.exit(1)

    if len(PARTICIPANTS) != 20:
        print("ERROR: There must be exactly 20 participants.")
        sys.exit(1)

    print()
    print("============================================")
    print("        PROVABLY FAIR DRAWING")
    print("============================================")
    print()

    print(f"Bitcoin network : Bitcoin mainnet")
    print(f"Block height    : {height}")
    print()

    # Get Bitcoin block hash
    block_hash = get_block_hash(height)

    print(f"Block hash      : {block_hash}")
    print()

    # Create deterministic RNG from Bitcoin hash
    rng = BitcoinRandom(block_hash)

    # Shuffle participant numbers
    participant_numbers = list(range(1, len(PARTICIPANTS) + 1))

    shuffled = deterministic_shuffle(
        participant_numbers,
        rng
    )

    print("RESULT")
    print("--------------------------------------------")

    first = shuffled[0]
    second = shuffled[1]

    print(
        f"FIRST PRIZE  : #{first} "
        f"{PARTICIPANTS[first - 1]}"
    )

    print(
        f"SECOND PRIZE : #{second} "
        f"{PARTICIPANTS[second - 1]}"
    )

    print()
    print("COMPLETE DETERMINISTIC ORDER")
    print("--------------------------------------------")

    for position, number in enumerate(shuffled, start=1):
        print(
            f"{position:2d}. "
            f"#{number:2d} "
            f"{PARTICIPANTS[number - 1]}"
        )

    print()
    print("============================================")
    print("Anyone can reproduce this result using:")
    print()
    print(f"  Bitcoin block height: {height}")
    print(f"  Bitcoin block hash:   {block_hash}")
    print()
    print("and the exact participant list and")
    print("draw.py program.")
    print("============================================")
    print()


if __name__ == "__main__":
    main()
