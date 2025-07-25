from typing import Dict, Any, List, Union
import requests
import pyarrow as pa
import pandas as pd
import geopandas as gpd
import numpy as np
import json
import io
import os
from requests_toolbelt.multipart import decoder
from shapely import wkb
from shapely.geometry import shape, mapping
from shapely.geometry.base import BaseGeometry

_table_html_limit = 20


class HugrIPCTable:
    path: str
    _geom_fields: Dict[str, Dict[str, str]]
    is_geo: bool
    _df: pd.DataFrame

    def __init__(
        self,
        path: str,
        batches: List[pa.RecordBatch],
        geom_fields: Dict[str, Dict[str, str]],
        is_geo: bool,
    ):
        self.path = path
        self._geom_fields = geom_fields
        self.is_geo = is_geo
        if len(batches) != 0:
            self._df = pa.Table.from_batches(batches).to_pandas()
        else:
            self._df = pd.DataFrame()
        # Decode first level geometry fields
        if is_geo:
            for field, fi in geom_fields.items():
                encoding = fi.get("format", "wkb").lower()
                if len(field.split(".")) == 1:
                    if encoding == "h3cell":
                        # H3 cells are stored as strings, no decoding needed
                        continue
                    self._df[field] = self._df[field].apply(
                        lambda x: _decode_geom(x, encoding)
                    )

    def df(self) -> pd.DataFrame:
        if self._df is None:
            raise ValueError("DataFrame not loaded")
        return self._df

    def to_geo_dataframe(self, field: str = None) -> gpd.GeoDataFrame:
        if not self.is_geo:
            raise ValueError("Table is not marked as geometry")
        if field is None:
            field = list(self._geom_fields.keys())[0] if self._geom_fields else None
        if field not in self._geom_fields:
            raise ValueError(f"Field {field} not found in geometry fields")

        fi = self._geom_fields[field]
        encoding = fi.get("format", "wkb").lower()
        srid = fi.get("srid")

        try:
            # Copy the DataFrame to avoid modifying the original
            df = self.df().copy()
            # Decode only nested geometry fields (in nested objects or arrays of objects)
            if '.' in field:
                df = flatten_to_field(df, field)
            df[field] = df[field].apply(lambda x: _decode_geom(x, encoding))
            gdf = gpd.GeoDataFrame(df, geometry=field)
            if srid:
                gdf.set_crs(srid, inplace=True)
            return gdf
        except Exception as e:
            print(f"[warn] Failed to decode geometry field {field}: {e}")

    def info(self) -> str:
        fields = self._df.columns.tolist()
        num_rows = len(self._df)
        num_cols = len(fields)

        return (
            f"<b>Rows:</b> {num_rows}<br>"
            f"<b>Columns:</b> {num_cols}<br>"
            f"<b>Has geometry:</b> {self.is_geo}<br>"
            f"<b>Geometry fields:</b> {len(self._geom_fields)}<br>"
            f"<b>Fields:</b> {', '.join(fields)}<br>"
            f"<b>Geometry Fields:</b> {', '.join(self._geom_fields.keys())}<br>"
        )

    def _repr_html_(self):
        preview_html = self._df.head(20).to_html(
            border=1, index=False
        )  # максимум 20 строк в предпросмотр

        return f"""
        <div>
            <b>HugrIPCTable</b><br/>
            <b>Path:</b> {self.path}<br/>
            {self.info()}
            <div style="margin-top:10px;">
                <a href="#" onclick="
                    const table = document.getElementById('table-{id(self)}');
                    const link = document.getElementById('link-{id(self)}');
                    if (table.style.display === 'none') {{
                        table.style.display = 'block';
                        link.innerText = 'Hide Table';
                    }} else {{
                        table.style.display = 'none';
                        link.innerText = 'Show Table';
                    }}
                    return false;">Show Table</a>
            </div>
            <div id="table-{id(self)}" style="display:none; max-height:600px; overflow:auto; border:1px solid #ccc; padding:10px; margin-top:5px;">
                {preview_html}
            </div>
        </div>
        """

    def geojson_layers(self):
        data = {}
        for field, fi in self._geom_fields.items():
            encoding = fi.get("format", "wkb").lower()
            df = self.df()
            if len(field.split(".")) > 1:
                df = flatten_to_field(df, field)

            features = []
            for _, row in df.iterrows():
                if field in row:
                    feature = {}
                    feature["type"] = "Feature"
                    feature["geometry"] = _encode_geojson(row[field], encoding)
                    feature["properties"] = row.drop(field).to_dict()
                    features.append(feature)

            data[field] = {
                "type": "FeatureCollection",
                "features": features,
            }
        return data

    # Transforms data frame to set of data frames with geometry columns as GeoJSON
    def df_with_geojson(self, field: str = None) -> Dict[str, pd.DataFrame]:
        # Create sorted list of geometry fields by number of levels (dots) and their names
        gff = sorted(
            [f for f in self._geom_fields.items() if f[0] == field or field is None],
            key=lambda f: (len(f[0].split(".")), f),
        )
        # processed paths
        paths = {}
        for field, fi in gff:
            encoding = fi.get("format", "wkb").lower()
            path = ".".join(field.split(".")[:-1])
            if '.' not in field:
                path = ""
            if path not in paths:
                df = self.df()
                if path == "":
                    df.copy()
                if path != "":
                    df = flatten_to_field(df, field)
                paths[path] = df
            else:
                df = paths[path]

            df[field] = df[field].apply(lambda x: _encode_geojson(x, encoding))
            self._geom_fields[field]["format"] = "geojson"
        return paths

    def explore_map(self, width=None, height=None):
        explore_map(self, width=width, height=height)


class HugrIPCObject:
    path: str
    _content: dict
    _geom_fields: Dict[str, Dict[str, str]]
    is_geo: bool

    def __init__(
        self,
        path: str,
        content: dict,
        geom_fields: Dict[str, Dict[str, str]] = None,
        is_geo: bool = False,
    ):
        self.path = path
        self._content = content
        if geom_fields is None:
            geom_fields = {}
        self._geom_fields = geom_fields
        self.is_geo = is_geo

    def content(self) -> dict:
        if self._content is None:
            raise ValueError("Content not loaded")
        return self._content

    def dict(self) -> dict:
        return self._content

    def df(self) -> pd.DataFrame:
        if self._content is None:
            raise ValueError("Content not loaded")

        return pd.DataFrame([self._content])

    def to_geo_dataframe(
        self, field: str = None, flatten: bool = True
    ) -> gpd.GeoDataFrame:
        if not self.is_geo:
            raise ValueError("Table is not marked as geometry")
        if field is None:
            field = list(self._geom_fields.keys())[0] if self._geom_fields else None
        if field not in self._geom_fields:
            raise ValueError(f"Field {field} not found in geometry fields")

        fi = self._geom_fields[field]
        encoding = fi.get("format", "wkb").lower()
        srid = fi.get("srid")

        try:
            # Copy the DataFrame to avoid modifying the original
            df = self.df().copy()
            # Decode only nested geometry fields (in nested objects or arrays of objects)
            if len(field.split(".")) > 1:
                df = flatten_to_field(df, field)
                df[field] = df[field].apply(lambda x: _decode_geom(x, encoding))
            gdf = gpd.GeoDataFrame(df, geometry=field)
            if srid:
                gdf.set_crs(srid, inplace=True)
            return gdf
        except Exception as e:
            print(f"[warn] Failed to decode geometry field {field}: {e}")

    def info(self) -> str:
        keys = list(self._content.keys())
        num_keys = len(keys)
        if self.is_geo:
            return (
                f"<b>Keys:</b> {num_keys}<br>"
                f"<b>Has geometry:</b> {self.is_geo}<br>"
                f"<b>Geometry fields:</b> {len(self._geom_fields)}<br>"
                f"<b>Keys:</b> {', '.join(keys)}<br>"
                f"<b>Geometry Fields:</b> {', '.join(self._geom_fields.keys())}<br>"
            )

        return f"<b>Keys:</b> {num_keys}<br>" f"<b>Keys:</b> {', '.join(keys)}<br>"

    def _repr_html_(self):
        pretty_json = json.dumps(self._content, indent=2, ensure_ascii=False)

        return f"""
        <div>
            <b>HugrIPCObject</b><br/>
            <b>Path:</b> {self.path}<br/>
            {self.info()}
            <div style="margin-top:10px;">
                <a href="#" onclick="document.getElementById('raw-json-{id(self)}').style.display='block'; this.style.display='none'; return false;">Show JSON</a>
            </div>
            <div id="raw-json-{id(self)}" style="display:none; max-height:500px; overflow:auto; border:1px solid #ccc; padding:10px; margin-top:5px; white-space:pre; font-family:monospace;">
                <div>{pretty_json}<div>
            </div>
        </div>
        """

    def geojson_layers(self):
        data = {}
        for field, fi in self._geom_fields.items():
            encoding = fi.get("format", "wkb").lower()
            df = self.df()
            if len(field.split(".")) > 1:
                df = flatten_to_field(df, field)

            features = []
            for _, row in df.iterrows():
                feature = {}
                if field in row:
                    feature["type"] = "Feature"
                    feature["geometry"] = _encode_geojson(row[field], encoding)
                    feature["properties"] = row.drop(field).to_dict()
                    features.append(feature)
            data[field] = {
                "type": "FeatureCollection",
                "features": features,
            }
        return data

    # Transforms data frame to set of data frames with geometry columns as GeoJSON
    def df_with_geojson(self, field: str = None) -> Dict[str, pd.DataFrame]:
        # Create sorted list of geometry fields by number of levels (dots) and their names
        gff = sorted(
            [f for f in self._geom_fields.items() if f[0] == field or field is None],
            key=lambda f: (len(f[0].split(".")), f),
        )
        # processed paths
        paths = {}
        for field, fi in gff:
            encoding = fi.get("format", "wkb").lower()
            path = ".".join(field.split(".")[:-1])
            if '.' not in field:
                path = ""
            if path not in paths:
                df = self.df()
                if path == "":
                    df.copy()
                if path != "":
                    df = flatten_to_field(df, field)
                paths[path] = df
            else:
                df = paths[path]

            df[field] = df[field].apply(lambda x: _encode_geojson(x, encoding))
            self._geom_fields[field]["format"] = "geojson"
        return paths

    def explore_map(self, width=None, height=None):
        explore_map(self, width=width, height=height)


def flatten_to_field(df: pd.DataFrame, field: str) -> pd.DataFrame:
    df = df.copy()
    parts = field.split(".")
    for idx, _ in enumerate(parts[:-1]):
        current_path = ".".join(parts[: idx + 1])
        if current_path == field:
            break
        if current_path not in df.columns:
            raise ValueError(f"Field {current_path} not found in DataFrame")

        df[current_path] = df[current_path].apply(
            lambda x: x.tolist() if isinstance(x, np.ndarray) else x
        )

        if df[current_path].dropna().apply(lambda x: isinstance(x, list)).any():
            df = df.explode(current_path).reset_index(drop=True)

        if df[current_path].dropna().apply(lambda x: isinstance(x, dict)).any():
            nested_df = pd.json_normalize(
                df[current_path].dropna(), sep=".", max_level=0
            )
            nested_df.columns = [f"{current_path}.{col}" for col in nested_df.columns]
            df = pd.concat(
                [
                    df.drop(columns=[current_path]).reset_index(drop=True),
                    nested_df.reset_index(drop=True),
                ],
                axis=1,
            )
    return df


def _decode_geom(val, fmt):
    if not val:
        return None
    if isinstance(val, BaseGeometry):
        return val
    if fmt == "h3cell":
        return val
    if fmt == "wkb":
        return wkb.loads(val)
    elif fmt == "geojson":
        return shape(val)
    elif fmt == "geojsonstring":
        return shape(json.loads(val))
    else:
        raise ValueError(f"Unknown geometry format: {fmt}")


def _encode_geojson(val, fmt):
    if not val:
        return None
    if isinstance(val, BaseGeometry):
        return mapping(val)
    if fmt == "h3cell":
        return val
    if fmt == "wkb":
        return mapping(wkb.load(val))
    elif fmt == "geojson":
        return val
    elif fmt == "geojsonstring":
        return json.loads(val)
    else:
        raise ValueError(f"Unknown geometry format: {fmt}")


class HugrIPCResponse:
    _parts: Dict[str, Union[HugrIPCTable, HugrIPCObject]]
    _extensions: Dict[str, HugrIPCObject]

    def __init__(self, response: requests.Response):
        self._parts, self._extensions = self._parse_multipart(response)

    def _parse_multipart(self, response: requests.Response):
        data = decoder.MultipartDecoder.from_response(response)
        parts: Dict[str, Union[HugrIPCTable, HugrIPCObject]] = {}
        extensions: Dict[str, HugrIPCObject] = {}
        for part in data.parts:
            headers = {k.decode(): v.decode() for k, v in part.headers.items()}
            path = headers.get("X-Hugr-Path")
            part_type = headers.get("X-Hugr-Part-Type")
            format = headers.get("X-Hugr-Format")
            if part_type == "error":
                raise ValueError(f"Error in part {path}: {part.content.decode()}")
            if format == "table":
                if headers.get("X-Hugr-Empty", "false") == "true":
                    parts[path] = HugrIPCTable(path, [], {}, False)
                    continue
                reader = pa.ipc.open_stream(io.BytesIO(part.content))
                batches = list(reader)
                geom_fields = json.loads(headers.get("X-Hugr-Geometry-Fields", "{}"))
                is_geo = headers.get("X-Hugr-Geometry", "false") == "true"
                parts[path] = HugrIPCTable(path, batches, geom_fields, is_geo)
            elif format == "object" and part_type == "data":
                content = json.loads(part.content)
                geom_fields = json.loads(headers.get("X-Hugr-Geometry-Fields", "{}"))
                is_geo = headers.get("X-Hugr-Geometry", "false") == "true"
                parts[path] = HugrIPCObject(path, content, geom_fields, is_geo)
            elif format == "object" and part_type == "extensions":
                content = json.loads(part.content)
                extensions[path] = HugrIPCObject(path, content)

        return parts, extensions

    @property
    def parts(self):
        return self._parts

    def __iter__(self):
        return iter(self._parts.keys())

    def __len__(self):
        return len(self._parts)

    def __contains__(self, key):
        return key in self._parts

    def __getitem__(self, key):
        return self._parts[key]

    def _part(self, path: str = None) -> Union[HugrIPCTable, HugrIPCObject]:
        part = self._parts.get(path)
        if path is None and len(self._parts) == 1:
            part = list(self._parts.values())[0]
        if not part:
            raise ValueError(f"No such path: {path}")
        return part

    def df(self, path: str = None) -> pd.DataFrame:
        part = self._part(path)
        if isinstance(part, HugrIPCTable):
            return part.df()
        elif isinstance(part, HugrIPCObject):
            return part.df()
        else:
            raise TypeError("Not a tabular format")

    def record(self, path: str = None):
        part = self._part(path)
        if not part:
            raise ValueError(f"No such path: {path}")
        elif isinstance(part, HugrIPCObject):
            return part.content()
        else:
            raise TypeError("Not a readable object")

    def gdf(self, path: str = None, field: str = None) -> gpd.GeoDataFrame:
        part = self._part(path)
        if not part:
            raise ValueError(f"No such path: {path}")
        if isinstance(part, HugrIPCTable):
            return part.to_geo_dataframe(field)
        elif isinstance(part, HugrIPCObject):
            return part.to_geo_dataframe(field)
        else:
            raise TypeError("Not a readable object")
        return None

    def extensions(self):
        return self._extensions

    def extension(self, path: str = None):
        if path is None and len(self._extensions) == 1:
            return list(self._extensions.values())[0]

        ext = self._extensions.get(path)
        if not ext:
            raise ValueError(f"No such path: {path}")
        return ext

    def __repr__(self):
        return f"HugrIPCResponse(data={self._parts}, extensions={self._extensions})"

    def _repr_html_(self):
        rows = ""
        for path, part in self.parts.items():
            ptype = (
                "Table"
                if isinstance(part, HugrIPCTable)
                else "Object" if isinstance(part, HugrIPCObject) else "Unknown"
            )
            info_html = part.info()
            rows += f"<tr><td>{path}</td><td>{ptype}</td><td>{info_html}</td></tr>"

        for path, ext in self.extensions().items():
            ext_type = "Extension"
            info_html = ext.info()
            rows += f"<tr><td>{path}</td><td>{ext_type}</td><td>{info_html}</td></tr>"

        return f"""
        <h3>HugrIPCResponse Overview</h3>
        <table border="1" cellpadding="5" cellspacing="0">
            <thead>
                <tr>
                    <th>Path</th>
                    <th>Content Type</th>
                    <th>Info</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        """

    def geojson_layers(self):
        features = {}
        for path, part in self.parts.items():
            for field, data in part.geojson_layers().items():
                features[path + "." + field] = data

        return features

    def df_with_geojson(
        self,
    ) -> Dict[str, pd.DataFrame]:
        paths = {}
        for path, part in self.parts.items():
            for pp, df in part.df_with_geojson().items():
                np = path
                if pp != "":
                    np += "." + pp
                paths[np] = df
        return paths

    def explore_map(self, width=None, height=None):
        explore_map(self, width=width, height=height)


class HugrClient:
    def __init__(
        self,
        url: str = None,
        api_key: str = None,
        token: str = None,
        role: str = None,
    ):
        if not url:
            url = os.environ.get("HUGR_URL")
            if not url:
                raise ValueError("HUGR_URL environment variable not set")
        if not api_key and not token:
            api_key = os.environ.get("HUGR_API_KEY")
            token = os.environ.get("HUGR_TOKEN")
        self._url = url
        self._api_key = api_key
        self._token = token
        self._role = role
        self._api_key_header = os.environ.get("HUGR_API_KEY_HEADER", "X-Hugr-Api-Key")
        self._role_header = os.environ.get("HUGR_ROLE_HEADER", "X-Hugr-Role")

    def _headers(self):
        headers = {"Accept": "multipart/mixed", "Content-Type": "application/json"}
        if self._api_key:
            headers[self._api_key_header] = self._api_key
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if self._role:
            headers[self._role_header] = self._role
        return headers

    def query(self, query: str, variables: dict = None):
        headers = self._headers()
        payload = {"query": query, "variables": variables or {}}
        resp = requests.post(self._url, headers=headers, json=payload)
        if resp.status_code == 500:
            raise ValueError(f"Server error: {resp.status_code} {resp.text}")
        resp.raise_for_status()
        return HugrIPCResponse(resp)

    def __repr__(self):
        return f"HugrClient(url={self._url}, api_key={self._api_key}, token={self._token}, role={self._role})"


def query(
    query: str,
    variables: dict = None,
    url: str = None,
    api_key: str = None,
    token: str = None,
    role: str = None,
):
    client = HugrClient(url=url, api_key=api_key, token=token, role=role)
    return client.query(query, variables)


def connect(
    url: str = None,
    api_key: str = None,
    token: str = None,
    role: str = None,
):
    return HugrClient(url, api_key, token, role)


def explore_map(
    object: Union[HugrIPCResponse, HugrIPCTable, HugrIPCObject], width=800, height=600
):
    data = object.df_with_geojson()
    from keplergl import KeplerGl

    m = KeplerGl(width=width, height=height)
    for path, layer in data.items():
        m.add_data(data=layer, name=path)
    return m
