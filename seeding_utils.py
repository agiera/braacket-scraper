import math
from datetime import date

import polars as pl


def build_matchup_matrix(
    matches: pl.DataFrame,
    entrant_urls: list[str],
    reference_date: str | date | None = None,
    half_life_days: float = 90.0,
) -> dict[tuple[str, str], float]:
    """
    Build a recency-weighted matchup frequency matrix from match history.

    Args:
        matches: DataFrame with columns winner_url, loser_url, tournament_date.
        entrant_urls: List of player URLs to include (filters to only these players).
        reference_date: Date to measure recency from. Defaults to today.
        half_life_days: Half-life in days for exponential decay weighting.

    Returns:
        Dict mapping (url_a, url_b) -> weighted matchup count (symmetric, canonical key order).
    """
    if reference_date is None:
        reference_date = date.today()
    elif isinstance(reference_date, str):
        reference_date = date.fromisoformat(reference_date)

    decay = math.log(2) / half_life_days
    url_set = set(entrant_urls)

    # Filter to matches involving two entrants
    relevant = (
        matches
        .filter(
            pl.col('winner_url').is_in(url_set),
            pl.col('loser_url').is_in(url_set),
        )
        .select('winner_url', 'loser_url', 'tournament_date')
        .with_columns(pl.col('tournament_date').cast(pl.Date))
    )

    matrix: dict[tuple[str, str], float] = {}
    for row in relevant.iter_rows(named=True):
        a, b = row['winner_url'], row['loser_url']
        if a == b:
            continue
        key = (min(a, b), max(a, b))
        days_ago = (reference_date - row['tournament_date']).days
        weight = math.exp(-decay * max(days_ago, 0))
        matrix[key] = matrix.get(key, 0.0) + weight

    return matrix


def get_matchup_weight(
    matrix: dict[tuple[str, str], float], url_a: str, url_b: str
) -> float:
    """Look up the matchup weight between two players."""
    key = (min(url_a, url_b), max(url_a, url_b))
    return matrix.get(key, 0.0)


def _next_power_of_2(n: int) -> int:
    """Return the smallest power of 2 >= n."""
    p = 1
    while p < n:
        p *= 2
    return p


def earliest_meeting_round(seed_a: int, seed_b: int, bracket_size: int) -> int:
    """
    Return the earliest round two seeds can meet in a standard single-elimination
    bracket (winners side of double-elim), assuming higher seeds always win.

    Seeds are 1-indexed. bracket_size must be a power of 2.

    Round 1 = first round, round log2(bracket_size) = finals.
    """
    if seed_a == seed_b:
        return 0
    n = bracket_size
    num_rounds = int(math.log2(n))

    # Build the standard bracket placement: map seed -> position (0-indexed)
    # In a standard bracket, the positions are filled recursively:
    # For bracket of size N, seed 1 is at position 0, seed N at position 1,
    # then each half is filled recursively.
    positions = _seed_positions(n)

    pos_a = positions.get(seed_a)
    pos_b = positions.get(seed_b)
    if pos_a is None or pos_b is None:
        return num_rounds + 1  # seeds outside bracket, shouldn't meet

    # Two positions meet in round r if they first share the same group of size 2^r
    for r in range(1, num_rounds + 1):
        group_size = 1 << r
        if pos_a // group_size == pos_b // group_size:
            return r

    return num_rounds


def _seed_positions(n: int) -> dict[int, int]:
    """
    Return a mapping from seed number (1-indexed) to bracket position (0-indexed)
    for a standard single-elimination bracket of size n (power of 2).

    Standard placement: 1 vs n in R1, and recursively separated so top seeds
    meet as late as possible.
    """
    bracket = _standard_bracket(n)
    return {seed: pos for pos, seed in enumerate(bracket)}


def _standard_bracket(n: int) -> list[int]:
    """Return list where result[position] = seed for a standard bracket of size n."""
    if n == 1:
        return [1]
    top = _standard_bracket(n // 2)
    result = []
    for seed in top:
        result.append(seed)
        result.append(n + 1 - seed)
    return result


def round_weight(round_num: int, num_rounds: int) -> float:
    """
    Return a cost weight for a matchup occurring in the given round.
    Earlier rounds get exponentially higher weight.
    Round 1 (first round) = highest weight, later rounds = lower.
    """
    if round_num <= 0 or round_num > num_rounds:
        return 0.0
    # Weight = 2^(num_rounds - round_num), so round 1 is heaviest
    # But cap: don't penalize rounds beyond quarterfinals much
    return 2.0 ** max(num_rounds - round_num - 2, 0)


def _precompute_round_weights(bracket_size: int) -> dict[tuple[int, int], float]:
    """
    Precompute round_weight for every pair of seeds in the bracket.
    Returns dict of (seed_a, seed_b) -> weight (canonical order: a < b).
    """
    num_rounds = int(math.log2(bracket_size))
    positions = _seed_positions(bracket_size)
    weights = {}
    for sa in range(1, bracket_size + 1):
        pos_a = positions.get(sa)
        if pos_a is None:
            continue
        for sb in range(sa + 1, bracket_size + 1):
            pos_b = positions.get(sb)
            if pos_b is None:
                continue
            for r in range(1, num_rounds + 1):
                group_size = 1 << r
                if pos_a // group_size == pos_b // group_size:
                    rw = round_weight(r, num_rounds)
                    if rw > 0:
                        weights[(sa, sb)] = rw
                    break
    return weights


def seeding_cost(
    seed_assignment: list[tuple[int, str]],
    matchup_matrix: dict[tuple[str, str], float],
    bracket_size: int,
) -> float:
    """
    Compute the total matchup-overlap cost of a seeding assignment.

    Args:
        seed_assignment: List of (seed_num, player_url) tuples.
        matchup_matrix: Output of build_matchup_matrix.
        bracket_size: Power-of-2 bracket size.

    Returns:
        Total weighted cost (lower is better).
    """
    rw_matrix = _precompute_round_weights(bracket_size)
    total = 0.0
    n = len(seed_assignment)
    for i in range(n):
        seed_i, url_i = seed_assignment[i]
        for j in range(i + 1, n):
            seed_j, url_j = seed_assignment[j]
            mw = get_matchup_weight(matchup_matrix, url_i, url_j)
            if mw == 0:
                continue
            key = (min(seed_i, seed_j), max(seed_i, seed_j))
            rw = rw_matrix.get(key, 0.0)
            total += mw * rw
    return total


def get_seed_tiers(num_seeds: int) -> list[tuple[int, int]]:
    """
    Return tier boundaries as (start_seed, end_seed) inclusive pairs.
    Standard tiers: 1-2, 3-4, 5-8, 9-16, 17-32, 33-64, 65-128, ...
    """
    tiers = [(1, 2), (3, 4)]
    size = 4
    low = 5
    while low <= num_seeds:
        high = min(low + size - 1, num_seeds)
        tiers.append((low, high))
        low = high + 1
        size *= 2
    return tiers


def optimize_seeding(
    entrant_seeding: pl.DataFrame,
    matchup_matrix: dict[tuple[str, str], float],
    url_col: str = 'url',
    seed_col: str = 'seed_num',
    bracket_size: int | None = None,
    max_iterations: int = 100,
    max_seed_distance: int | None = None,
) -> pl.DataFrame:
    """
    Optimize a rating-based seeding by swapping players within tiers to
    minimize repeated matchups in early bracket rounds.

    Args:
        entrant_seeding: DataFrame with at least seed_col and url_col columns.
        matchup_matrix: Output of build_matchup_matrix.
        url_col: Column name for player URL identifiers.
        seed_col: Column name for seed numbers (1-indexed).
        bracket_size: Power-of-2 bracket size. If None, auto-computed.
        max_iterations: Maximum hill-climbing iterations.
        max_seed_distance: Maximum number of positions a player can move from
            their original seed. None means no limit (tier boundaries still apply).

    Returns:
        Tuple of (updated DataFrame, list of swaps made, final cost).
    """
    seeds = entrant_seeding.select(seed_col, url_col).sort(seed_col).to_dicts()
    num_entrants = len(seeds)

    if bracket_size is None:
        bracket_size = _next_power_of_2(num_entrants)

    tiers = get_seed_tiers(num_entrants)

    # Precompute round weight matrix once
    rw_matrix = _precompute_round_weights(bracket_size)

    # urls[i] = url of player at seed (i+1)
    urls = [d[url_col] for d in seeds]
    valid_indices = {i for i, u in enumerate(urls) if u is not None}

    # Track each URL's original seed index for max_seed_distance constraint
    url_original_idx = {u: i for i, u in enumerate(urls) if u is not None}

    def player_cost_contribution(idx: int) -> float:
        """Cost contributed by player at index idx against all other players."""
        url_a = urls[idx]
        if url_a is None:
            return 0.0
        seed_a = idx + 1
        total = 0.0
        for j in valid_indices:
            if j == idx:
                continue
            url_b = urls[j]
            mw = get_matchup_weight(matchup_matrix, url_a, url_b)
            if mw == 0:
                continue
            seed_b = j + 1
            key = (min(seed_a, seed_b), max(seed_a, seed_b))
            rw = rw_matrix.get(key, 0.0)
            total += mw * rw
        return total

    # Compute initial total cost
    best_cost = 0.0
    valid_list = sorted(valid_indices)
    for ii in range(len(valid_list)):
        idx_i = valid_list[ii]
        url_i = urls[idx_i]
        seed_i = idx_i + 1
        for jj in range(ii + 1, len(valid_list)):
            idx_j = valid_list[jj]
            url_j = urls[idx_j]
            mw = get_matchup_weight(matchup_matrix, url_i, url_j)
            if mw == 0:
                continue
            seed_j = idx_j + 1
            key = (min(seed_i, seed_j), max(seed_i, seed_j))
            rw = rw_matrix.get(key, 0.0)
            best_cost += mw * rw

    initial_cost = best_cost
    swaps_made = []

    for iteration in range(max_iterations):
        improved = False
        for tier_low, tier_high in tiers:
            tier_indices = [
                s - 1 for s in range(tier_low, tier_high + 1)
                if s - 1 < num_entrants and s - 1 in valid_indices
            ]
            if len(tier_indices) < 2:
                continue

            for ii in range(len(tier_indices)):
                for jj in range(ii + 1, len(tier_indices)):
                    idx_a = tier_indices[ii]
                    idx_b = tier_indices[jj]

                    # Check max_seed_distance: would this swap push either
                    # player too far from their original seed?
                    if max_seed_distance is not None:
                        url_a_cur, url_b_cur = urls[idx_a], urls[idx_b]
                        orig_a = url_original_idx.get(url_a_cur, idx_a)
                        orig_b = url_original_idx.get(url_b_cur, idx_b)
                        # After swap: url_a goes to idx_b, url_b goes to idx_a
                        if (abs(orig_a - idx_b) > max_seed_distance or
                                abs(orig_b - idx_a) > max_seed_distance):
                            continue

                    # Compute cost delta from swapping: O(n) instead of O(n^2)
                    # Remove old contributions of both players
                    old_cost_a = player_cost_contribution(idx_a)
                    old_cost_b = player_cost_contribution(idx_b)
                    # Their mutual cost is double-counted, subtract once
                    url_a, url_b = urls[idx_a], urls[idx_b]
                    mutual_mw = get_matchup_weight(matchup_matrix, url_a, url_b)
                    seed_a, seed_b = idx_a + 1, idx_b + 1
                    mutual_key = (min(seed_a, seed_b), max(seed_a, seed_b))
                    mutual_rw = rw_matrix.get(mutual_key, 0.0)
                    old_contribution = old_cost_a + old_cost_b - mutual_mw * mutual_rw

                    # Swap
                    urls[idx_a], urls[idx_b] = urls[idx_b], urls[idx_a]

                    new_cost_a = player_cost_contribution(idx_a)
                    new_cost_b = player_cost_contribution(idx_b)
                    new_contribution = new_cost_a + new_cost_b - mutual_mw * mutual_rw

                    new_cost = best_cost - old_contribution + new_contribution
                    if new_cost < best_cost - 1e-9:
                        swaps_made.append({
                            'seed_a': seed_a,
                            'seed_b': seed_b,
                            'url_a': urls[idx_a],
                            'url_b': urls[idx_b],
                            'cost_before': best_cost,
                            'cost_after': new_cost,
                        })
                        best_cost = new_cost
                        improved = True
                    else:
                        # Revert
                        urls[idx_a], urls[idx_b] = urls[idx_b], urls[idx_a]

        if not improved:
            break

    # Build result: map url -> new seed_num
    url_to_new_seed = {u: i + 1 for i, u in enumerate(urls)}

    result = (
        entrant_seeding
        .with_columns(pl.col(seed_col).alias('original_seed'))
        .with_columns(
            pl.col(url_col)
            .map_elements(lambda u: url_to_new_seed.get(u, None), return_dtype=pl.Int64)
            .alias(seed_col)
        )
        .sort(seed_col)
    )

    return result, swaps_made, best_cost


def format_swap_report(
    swaps: list[dict],
    entrant_seeding: pl.DataFrame,
    url_col: str = 'url',
    tag_col: str = 'gamertag',
) -> str:
    """Format a human-readable report of swaps made during optimization."""
    if not swaps:
        return "No swaps needed — seeding is already optimal for matchup separation."

    url_to_tag = {}
    if tag_col in entrant_seeding.columns:
        for row in entrant_seeding.iter_rows(named=True):
            url_to_tag[row[url_col]] = row.get(tag_col, row[url_col])

    lines = [f"Swaps made: {len(swaps)}"]
    for s in swaps:
        tag_a = url_to_tag.get(s['url_a'], s['url_a'])
        tag_b = url_to_tag.get(s['url_b'], s['url_b'])
        cost_reduction = s['cost_before'] - s['cost_after']
        lines.append(
            f"  Seed {s['seed_a']} ↔ {s['seed_b']}: "
            f"{tag_a} ↔ {tag_b} "
            f"(cost −{cost_reduction:.2f})"
        )
    return "\n".join(lines)
