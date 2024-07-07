import asyncio
import numpy as np
import functools
from typing import Any, Callable, Coroutine


def exponential_delay(exponential_rate: float) -> Callable:
    """Wrapper for exponential tool."""

    def decorator(
        async_func: Callable,
    ) -> Callable[[Any, Any], Coroutine[Any, Any, str]]:
        @functools.wraps(async_func)
        async def wrapper(*args: Any, **kwargs: Any) -> str:
            # random delay
            delay = np.random.exponential(exponential_rate)
            print(f"waiting for {delay} seconds", flush=True)
            await asyncio.sleep(delay)
            return await async_func(*args, **kwargs)

        return wrapper

    return decorator


async def main() -> None:
    @exponential_delay(2)
    async def get_the_secret_fact() -> str:
        """Returns the secret fact."""
        return "The secret fact is: A baby llama is called a 'Cria'."

    output = await get_the_secret_fact()
    print(output)
    print(get_the_secret_fact.__doc__)


if __name__ == "__main__":
    asyncio.run(main())
