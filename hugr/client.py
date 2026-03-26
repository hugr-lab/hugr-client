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


def _parse_srid(srid_str: str) -> int:
    """Parse SRID from 'EPSG:4326', '4326', or empty string."""
    s = str(srid_str).replace("EPSG:", "").strip()
    try:
        return int(s) if s else 4326
    except ValueError:
        return 4326

# Lazy detection of spool proxy (JupyterLab with hugr-perspective-viewer)
_spool_proxy_checked = False
_spool_proxy_available = False


def _has_spool_proxy() -> bool:
    """Check if we're running inside Jupyter with spool proxy available."""
    global _spool_proxy_checked, _spool_proxy_available
    if _spool_proxy_checked:
        return _spool_proxy_available
    _spool_proxy_checked = True
    try:
        from jupyter_server import serverapp  # noqa: F401
        _spool_proxy_available = True
    except ImportError:
        _spool_proxy_available = False
    return _spool_proxy_available


class HugrIPCTable:
    path: str
    _geom_fields: Dict[str, Dict[str, str]]
    is_geo: bool

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
        self._batches = batches
        self._schema = batches[0].schema if batches else pa.schema([])
        self._spool_id = None

    def to_arrow(self) -> pa.Table:
        """Return pyarrow Table. Zero-copy from original batches."""
        if not self._batches:
            return pa.table({})
        return pa.Table.from_batches(self._batches)

    def df(self) -> pd.DataFrame:
        """Convert to pandas DataFrame. Fresh copy each call."""
        if not self._batches:
            return pd.DataFrame()
        df = pa.Table.from_batches(self._batches).to_pandas()
        # Decode first-level geometry fields
        if self.is_geo:
            for field, fi in self._geom_fields.items():
                encoding = fi.get("format", "wkb").lower()
                if len(field.split(".")) == 1:
                    if encoding == "h3cell":
                        continue
                    df[field] = df[field].apply(
                        lambda x: _decode_geom(x, encoding)
                    )
        return df

    def to_geo_dataframe(self, field: str = None) -> gpd.GeoDataFrame:
        """Convert to GeoDataFrame. Fresh copy each call."""
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
            # Convert from raw Arrow (not df() which already decodes geometry)
            if not self._batches:
                return gpd.GeoDataFrame()
            df = pa.Table.from_batches(self._batches).to_pandas()
            # Flatten nested geometry if needed
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
        fields = [f.name for f in self._schema]
        num_rows = sum(b.num_rows for b in self._batches)
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
        preview_html = self.df().head(20).to_html(
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

    def _repr_mimebundle_(self, **kwargs):
        """Jupyter display: Perspective viewer if spool proxy available, else HTML."""
        if _has_spool_proxy():
            spool_id = self._ensure_spool()
            if spool_id:
                metadata = {
                    "parts": [{
                        "id": spool_id,
                        "type": "arrow",
                        "title": self.path or "Result",
                        "spool_id": spool_id,
                        "pin_disabled": True,
                        "arrow_url": f"/hugr/spool/arrow/stream?q={spool_id}",
                        "rows": sum(b.num_rows for b in self._batches),
                        "columns": [{"name": f.name, "type": str(f.type)} for f in self._schema],
                        "geometry_columns": [
                            {"name": name, "srid": _parse_srid(meta.get("srid", "4326")), "format": "GeoArrow" if meta.get("format", "wkb").lower() == "wkb" else meta.get("format", "WKB")}
                            for name, meta in self._geom_fields.items()
                        ] if self.is_geo else [],
                    }],
                    "query_id": spool_id,
                    "arrow_url": f"/hugr/spool/arrow/stream?q={spool_id}",
                    "rows": sum(b.num_rows for b in self._batches),
                    "pin_disabled": True,
                }
                return {
                    "application/vnd.hugr.result+json": metadata,
                    "text/html": self._repr_html_(),
                }
        return {"text/html": self._repr_html_()}

    def _ensure_spool(self) -> str | None:
        """Write spool file if not yet written."""
        if self._spool_id is None and self._batches:
            try:
                from .spool import write_spool
                self._spool_id = write_spool(
                    self._batches, self._schema,
                    geom_fields=self._geom_fields if self.is_geo else None,
                )
            except Exception as e:
                import sys
                print(f"[hugr-client] Failed to write spool: {e}", file=sys.stderr)
                return None
        return self._spool_id

    def __del__(self):
        """Cleanup spool file on garbage collection."""
        if getattr(self, '_spool_id', None):
            try:
                from .spool import delete_spool
                delete_spool(self._spool_id)
            except Exception:
                pass

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

    def _repr_mimebundle_(self, **kwargs):
        """Jupyter display: Perspective viewer with all parts, like hugr-kernel."""
        if not _has_spool_proxy():
            return {"text/html": self._repr_html_()}

        parts_meta = []
        first_arrow = None

        for path, part in self.parts.items():
            if isinstance(part, HugrIPCTable) and part._batches:
                spool_id = part._ensure_spool()
                if not spool_id:
                    continue
                if first_arrow is None:
                    first_arrow = (spool_id, part)
                parts_meta.append({
                    "id": spool_id,
                    "type": "arrow",
                    "title": path,
                    "spool_id": spool_id,
                    "pin_disabled": True,
                    "arrow_url": f"/hugr/spool/arrow/stream?q={spool_id}",
                    "rows": sum(b.num_rows for b in part._batches),
                    "columns": [{"name": f.name, "type": str(f.type)} for f in part._schema],
                    "geometry_columns": [
                        {"name": name, "srid": _parse_srid(meta.get("srid", "4326")),
                         "format": "GeoArrow" if meta.get("format", "wkb").lower() == "wkb" else meta.get("format", "WKB")}
                        for name, meta in part._geom_fields.items()
                    ] if part.is_geo else [],
                })
            elif isinstance(part, HugrIPCObject):
                parts_meta.append({
                    "id": path,
                    "type": "json",
                    "title": path,
                    "data": part.dict(),
                })

        # Extensions (metadata from query response)
        for path, ext in self.extensions().items():
            parts_meta.append({
                "id": path,
                "type": "json",
                "title": path,
                "data": ext.dict(),
            })

        if not parts_meta:
            return {"text/html": self._repr_html_()}

        metadata = {"parts": parts_meta}
        # Backward-compatible flat fields from first Arrow part
        if first_arrow:
            sid, p = first_arrow
            metadata["query_id"] = sid
            metadata["arrow_url"] = f"/hugr/spool/arrow/stream?q={sid}"
            metadata["rows"] = sum(b.num_rows for b in p._batches)

        return {
            "application/vnd.hugr.result+json": metadata,
            "text/html": self._repr_html_(),
        }

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
        api_key_header: str = None,
        token: str = None,
        role: str = None,
        connection: str = None,
    ):
        self._connection_name = None

        # Priority 1: named connection from connections.json
        if connection is not None:
            self._apply_connection(connection if isinstance(connection, str) else None,
                                   connection if isinstance(connection, dict) else None,
                                   url, api_key, api_key_header, token, role)
        else:
            # Priority 2: explicit args + env vars
            if not url:
                url = os.environ.get("HUGR_URL")
            if not url:
                # Priority 3: default connection from connections.json
                try:
                    from .connections import get_connection
                    conn = get_connection()
                    self._apply_connection(None, conn, url, api_key, api_key_header, token, role)
                    return
                except (ValueError, FileNotFoundError):
                    raise ValueError(
                        "No URL provided. Set HUGR_URL env, pass url=, "
                        "or configure a connection in ~/.hugr/connections.json"
                    )
            if not api_key and not token:
                api_key = os.environ.get("HUGR_API_KEY")
                token = os.environ.get("HUGR_TOKEN")
            self._url = url
            self._api_key = api_key
            self._token = token
            self._role = role
            self._api_key_header = (
                api_key_header
                or os.environ.get("HUGR_API_KEY_HEADER", "X-Hugr-Api-Key")
            )
            self._role_header = os.environ.get("HUGR_ROLE_HEADER", "X-Hugr-Role")

    def _apply_connection(self, name, conn_dict, url, api_key, api_key_header, token, role):
        """Apply connection config from connections.json, with explicit args taking priority."""
        if conn_dict is None:
            from .connections import get_connection
            conn_dict = get_connection(name)

        self._connection_name = conn_dict.get("name")
        self._url = url or conn_dict.get("url")
        self._role = role or conn_dict.get("role")
        self._role_header = os.environ.get("HUGR_ROLE_HEADER", "X-Hugr-Role")

        auth_type = conn_dict.get("auth_type", "public")
        if auth_type == "api_key" and not api_key:
            self._api_key = conn_dict.get("api_key")
            self._api_key_header = (
                api_key_header
                or conn_dict.get("api_key_header")
                or os.environ.get("HUGR_API_KEY_HEADER", "X-Hugr-Api-Key")
            )
        else:
            self._api_key = api_key
            self._api_key_header = (
                api_key_header
                or os.environ.get("HUGR_API_KEY_HEADER", "X-Hugr-Api-Key")
            )

        if auth_type in ("bearer", "hub", "browser") and not token:
            self._token = (
                (conn_dict.get("tokens") or {}).get("access_token")
                or conn_dict.get("token")
            )
        else:
            self._token = token

        if not self._url:
            raise ValueError(f"Connection '{self._connection_name}' has no URL")

    @classmethod
    def from_connection(cls, name: str = None, **kwargs):
        """Create client from a named connection in ~/.hugr/connections.json."""
        from .connections import get_connection
        conn = get_connection(name)
        return cls(connection=conn, **kwargs)

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

        # Token may have been refreshed by connection service — re-read and retry
        if resp.status_code == 401 and self._connection_name:
            try:
                from .connections import get_connection
                conn = get_connection(self._connection_name)
                new_token = (conn.get("tokens") or {}).get("access_token")
                if new_token and new_token != self._token:
                    self._token = new_token
                    headers = self._headers()
                    resp = requests.post(self._url, headers=headers, json=payload)
            except (ValueError, FileNotFoundError):
                pass

        if resp.status_code == 401:
            raise PermissionError(
                "Authentication failed (401). Token expired or invalid. "
                "Re-login via connection manager or check credentials."
            )
        if resp.status_code == 403:
            raise PermissionError(
                "Access denied (403). Insufficient permissions."
            )
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
    api_key_header: str = None,
    token: str = None,
    role: str = None,
):
    client = HugrClient(url=url, api_key=api_key, api_key_header=api_key_header, token=token, role=role)
    return client.query(query, variables)


def connect(
    url: str = None,
    api_key: str = None,
    api_key_header: str = None,
    token: str = None,
    role: str = None,
):
    return HugrClient(url, api_key, api_key_header, token, role)


def explore_map(
    object: Union[HugrIPCResponse, HugrIPCTable, HugrIPCObject], width=800, height=600
):
    try:
        from keplergl import KeplerGl
    except ImportError:
        raise ImportError(
            "keplergl is required for explore_map(). "
            "Install with: pip install hugr-client[viz]"
        )
    data = object.df_with_geojson()
    m = KeplerGl(width=width, height=height)
    for path, layer in data.items():
        m.add_data(data=layer, name=path)
    return m
