from typing import Dict, Any, List
from collections import defaultdict

# user_state[user_id] = dict or None
user_state: Dict[int, Dict[str, Any] | None] = {}

# exercise_log[user_id][date_str] = [ {name, amount, unit, timestamp}, ... ]
exercise_log: Dict[int, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
