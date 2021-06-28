import requests
from pathlib import Path
from whdtscraper import fetch_dumps, fetch_latest_version, WIKI_URL

WIKI_DIR = Path.cwd().joinpath('wiki_dumps')


def download_if_needed(file_path: str, tsv_url: str) -> None:
    file_path_obj = WIKI_DIR.joinpath(file_path)

    if not file_path_obj.is_file():
        file_path_obj.parent.mkdir(exist_ok=True, parents=True)
        response = requests.get(tsv_url, allow_redirects=True)
        open(file_path_obj, 'wb').write(response.content)


def sync_wikies(lang: str, /, version: str = 'latest'):
    if (version == 'latest'):
        last_version_obj = fetch_latest_version()
        version = last_version_obj['version']

    wiki = f'{lang}wiki'
    dumps = fetch_dumps(version, wiki)

    for dump in dumps:
        url: str = dump['url']
        download_if_needed(url.replace(f'{WIKI_URL}/', ''), url)


def get_tsv_files(lang: str, /, version: str = 'latest') -> list[Path]:
    if version == 'latest':
        version_path = list(WIKI_DIR.glob('*'))[0]
    else:
        version_path = WIKI_DIR.joinpath(version)

    wiki = f'{lang}wiki'
    wiki_path = version_path.joinpath(wiki)
    files = [file for file in wiki_path.iterdir() if file.is_file()]
    return sorted(files, key=lambda f: f.stem)
