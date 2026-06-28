import asyncio, json, sys, os
from f2.apps.douyin.handler import DouyinHandler

async def fetch(webcast_id):
    cookie = os.environ.get("DOUYIN_COOKIE", "")
    kwargs = {
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
        },
        "proxies": {"http://": None, "https://": None},
        "cookie": cookie,
    }
    try:
        live = await asyncio.wait_for(
            DouyinHandler(kwargs).fetch_user_live_videos(webcast_id=webcast_id),
            timeout=10
        )
        raw = live._to_raw()
    except asyncio.TimeoutError:
        print(json.dumps({"error": "F2 timeout - cookie may be missing or invalid"}))
        return
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        return

    room = raw.get("data", {}).get("data", {})
    owner = room.get("user") or {}
    stream = (room.get("data") or [None])[0] or {}
    info = {
        "room_status": room.get("room_status", 0),
        "nickname": owner.get("nickname") or stream.get("owner", {}).get("nickname") or "",
        "avatar": (owner.get("avatar_thumb") or {}).get("url_list", [None])[0]
                 or (stream.get("owner") or {}).get("avatar_thumb", {}).get("url_list", [None])[0]
                 or "",
        "title": stream.get("title") or "",
        "like_count": stream.get("like_count") or 0,
        "room_id": stream.get("id_str") or "",
        "sec_uid": owner.get("sec_uid") or (stream.get("owner") or {}).get("sec_uid") or "",
        "follower_count": (owner.get("follower_info") or stream.get("owner") or {}).get("follower_count") or None,
        "viewer_count": (stream.get("room_view_stats") or {}).get("display_value") or None,
        "webcast_id": webcast_id,
    }
    print(json.dumps(info, ensure_ascii=False))

if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else ""
    asyncio.run(fetch(wid))
