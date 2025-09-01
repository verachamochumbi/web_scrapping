import argparse
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import praw
from dotenv import load_dotenv


DEFAULT_SUBREDDITS = [
    "politics",
    "PoliticalDiscussion",
    "worldnews",
]


def load_config_from_env() -> Dict[str, str]:
    """Load Reddit API credentials and user agent from environment variables.

    Expected variables:
      - REDDIT_CLIENT_ID
      - REDDIT_CLIENT_SECRET
      - REDDIT_USERNAME
      - REDDIT_PASSWORD
      - REDDIT_USER_AGENT
    """
    load_dotenv()

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    missing = [
        name
        for name, value in [
            ("REDDIT_CLIENT_ID", client_id),
            ("REDDIT_CLIENT_SECRET", client_secret),
            ("REDDIT_USERNAME", username),
            ("REDDIT_PASSWORD", password),
            ("REDDIT_USER_AGENT", user_agent),
        ]
        if not value
    ]
    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "user_agent": user_agent,
    }


def get_reddit_client() -> praw.Reddit:
    """Instantiate and return an authenticated PRAW Reddit client."""
    cfg = load_config_from_env()
    reddit = praw.Reddit(
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        username=cfg["username"],
        password=cfg["password"],
        user_agent=cfg["user_agent"],
    )
    # Simple sanity check: ensure read-only can be disabled (we need script capabilities)
    if reddit.read_only:
        # PRAW marks script apps as not read-only when username/password are provided
        # but if it still shows read_only=True, alert the user.
        raise RuntimeError(
            "Reddit client is read-only. Ensure you are using a 'script' app and provided username/password."
        )
    return reddit


def fetch_posts_for_subreddit(
    reddit: praw.Reddit,
    subreddit_name: str,
    mode: str = "hot",
    limit: int = 20,
) -> List[Dict[str, object]]:
    """Fetch posts for a given subreddit and return extracted fields.

    Extracted fields: title, score, num_comments, id, url, subreddit
    """
    subreddit = reddit.subreddit(subreddit_name)
    if mode == "hot":
        submissions = subreddit.hot(limit=limit)
    elif mode == "top":
        submissions = subreddit.top(limit=limit)
    else:
        raise ValueError("mode must be 'hot' or 'top'")

    posts: List[Dict[str, object]] = []
    for submission in submissions:
        posts.append(
            {
                "subreddit": subreddit_name,
                "id": submission.id,
                "title": submission.title,
                "score": int(submission.score),
                "num_comments": int(submission.num_comments),
                "url": submission.url,
            }
        )
    return posts


def fetch_top_comments_for_posts(
    reddit: praw.Reddit,
    posts: List[Dict[str, object]],
    comments_per_post: int = 5,
    subset_size: Optional[int] = 10,
) -> List[Dict[str, object]]:
    """Fetch up to `comments_per_post` top comments for a subset of the most relevant posts.

    The subset is determined by highest post score. If subset_size is None, use all posts.
    Extracted fields: body, score, post_id
    """
    if subset_size is not None and subset_size > 0:
        # Sort by score descending and select top N
        sorted_posts = sorted(posts, key=lambda p: int(p.get("score", 0)), reverse=True)
        selected_posts = sorted_posts[: subset_size]
    else:
        selected_posts = list(posts)

    comments: List[Dict[str, object]] = []
    for post in selected_posts:
        submission = reddit.submission(id=str(post["id"]))
        try:
            submission.comment_sort = "top"
            submission.comments.replace_more(limit=0)
            count = 0
            for comment in submission.comments:
                if hasattr(comment, "body"):
                    comments.append(
                        {
                            "post_id": post["id"],
                            "body": comment.body,
                            "score": int(getattr(comment, "score", 0)),
                        }
                    )
                    count += 1
                if count >= comments_per_post:
                    break
        except Exception as exc:  # noqa: BLE001
            # Continue with next post if any issue arises (e.g., deleted, locked)
            print(f"Warning: failed to fetch comments for post {post['id']}: {exc}")
            continue

    return comments


def ensure_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def save_csv_rows(rows: List[Dict[str, object]], csv_path: Path, columns: List[str]) -> None:
    if not rows:
        # Create empty CSV with headers for consistency
        pd.DataFrame(columns=columns).to_csv(csv_path, index=False)
        return
    df = pd.DataFrame(rows)
    # Reorder/limit columns if specified
    df = df[columns]
    df.to_csv(csv_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Reddit posts and comments using PRAW and save to CSV."
    )
    parser.add_argument(
        "--subreddits",
        nargs="+",
        default=DEFAULT_SUBREDDITS,
        help="List of subreddit names (without r/)",
    )
    parser.add_argument(
        "--mode",
        choices=["hot", "top"],
        default="hot",
        help="Which listing to use for posts",
    )
    parser.add_argument(
        "--posts-per-subreddit",
        type=int,
        default=20,
        help="Number of posts to fetch per subreddit",
    )
    parser.add_argument(
        "--comments-per-post",
        type=int,
        default=5,
        help="Number of comments to fetch per selected post",
    )
    parser.add_argument(
        "--subset-size",
        type=int,
        default=10,
        help=(
            "Number of most relevant posts (by score) to fetch comments for. "
            "Use 0 to fetch comments for all fetched posts."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "output"),
        help="Directory to write CSV outputs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    ensure_output_dir(output_dir)

    reddit = get_reddit_client()

    all_posts: List[Dict[str, object]] = []
    for subreddit in args.subreddits:
        print(f"Fetching {args.mode} posts from r/{subreddit} ...")
        posts = fetch_posts_for_subreddit(
            reddit=reddit,
            subreddit_name=subreddit,
            mode=args.mode,
            limit=args.posts_per_subreddit,
        )
        all_posts.extend(posts)

    print("Selecting posts for comment collection and fetching comments ...")
    comments = fetch_top_comments_for_posts(
        reddit=reddit,
        posts=all_posts,
        comments_per_post=args.comments_per_post,
        subset_size=(None if args.subset_size == 0 else args.subset_size),
    )

    posts_csv = output_dir / "posts.csv"
    comments_csv = output_dir / "comments.csv"

    print(f"Writing posts to {posts_csv} ...")
    save_csv_rows(
        rows=all_posts,
        csv_path=posts_csv,
        columns=["subreddit", "id", "title", "score", "num_comments", "url"],
    )

    print(f"Writing comments to {comments_csv} ...")
    save_csv_rows(
        rows=comments,
        csv_path=comments_csv,
        columns=["post_id", "body", "score"],
    )

    print("Done.")


if __name__ == "__main__":
    main()


