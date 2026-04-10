"""Business logic for the tabular dashboard module."""

from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta

from supabase import Client

from core.auth import UserContext


def _safe_float(value: object) -> float:
	if value in (None, ""):
		return 0.0
	try:
		return float(value)
	except (TypeError, ValueError):
		return 0.0


def _fetch_all(
	sb: Client,
	table: str,
	select: str,
	*,
	eq_filters: dict[str, object] | None = None,
	in_filters: dict[str, list[object]] | None = None,
	gte_filters: dict[str, object] | None = None,
	lte_filters: dict[str, object] | None = None,
	order_by: tuple[str, bool] | None = None,
) -> list[dict]:
	rows: list[dict] = []
	offset = 0
	while True:
		query = sb.table(table).select(select)
		for field, value in (eq_filters or {}).items():
			query = query.eq(field, value)
		for field, values in (in_filters or {}).items():
			if values:
				query = query.in_(field, values)
		for field, value in (gte_filters or {}).items():
			query = query.gte(field, value)
		for field, value in (lte_filters or {}).items():
			query = query.lte(field, value)
		if order_by:
			query = query.order(order_by[0], desc=order_by[1])
		batch = query.range(offset, offset + 9999).execute()
		data = batch.data or []
		rows.extend(data)
		if len(data) < 10000:
			break
		offset += 10000
	return rows


def _first_day_of_month(value: date) -> date:
	return value.replace(day=1)


def _last_day_of_month(value: date) -> date:
	return value.replace(day=calendar.monthrange(value.year, value.month)[1])


def _parse_iso_date(value: object) -> date | None:
	if not value:
		return None
	if isinstance(value, date):
		return value
	try:
		return date.fromisoformat(str(value))
	except ValueError:
		return None


def _month_shift(value: date, months: int) -> date:
	year = value.year
	month = value.month + months
	while month <= 0:
		month += 12
		year -= 1
	while month > 12:
		month -= 12
		year += 1
	day = min(value.day, calendar.monthrange(year, month)[1])
	return date(year, month, day)


def _business_days_between(start: date, end: date, holidays: set[date]) -> int:
	if end < start:
		return 0
	total = 0
	current = start
	while current <= end:
		if current.weekday() < 5 and current not in holidays:
			total += 1
		current += timedelta(days=1)
	return total


def _get_scope(sb: Client, user: UserContext) -> dict[str, object]:
	scope = {"carteras": None, "supervisor": None}
	if user.rol == "vendedor" and user.cartera:
		scope["carteras"] = [user.cartera]
	elif user.rol == "supervisor" and user.nombre:
		scope["supervisor"] = user.nombre
		rows = _fetch_all(
			sb,
			"supervisores",
			"cartera",
			eq_filters={"supervisor": user.nombre},
		)
		carteras = sorted({row.get("cartera") for row in rows if row.get("cartera")})
		if carteras:
			scope["carteras"] = carteras
	return scope


def _summary_rows_for_month(sb: Client, scope: dict[str, object], latest_date: date) -> tuple[list[dict], bool]:
	eq_filters = {"mes": latest_date.month, "anio": latest_date.year}
	in_filters = {}
	if scope.get("carteras"):
		in_filters["cartera"] = scope["carteras"]
	if scope.get("supervisor"):
		eq_filters["supervisor"] = scope["supervisor"]

	try:
		rows = _fetch_all(
			sb,
			"resumen_mes",
			"categoria,total_neto,total_kilos,pdv_activos",
			eq_filters=eq_filters,
			in_filters=in_filters,
		)
		return rows, True
	except Exception:
		rows = _fetch_all(
			sb,
			"resumen_mes",
			"categoria,total_neto,pdv_activos",
			eq_filters=eq_filters,
			in_filters=in_filters,
		)
		return rows, False


def _ventas_filters_for_scope(scope: dict[str, object]) -> tuple[dict[str, object], dict[str, list[object]]]:
	eq_filters = {}
	in_filters = {}
	if scope.get("carteras"):
		in_filters["cartera"] = scope["carteras"]
	elif scope.get("supervisor"):
		eq_filters["supervisor"] = scope["supervisor"]
	return eq_filters, in_filters


def _latest_sales_date(sb: Client, scope: dict[str, object]) -> date | None:
	eq_filters, in_filters = _ventas_filters_for_scope(scope)
	query = sb.table("ventas").select("fecha_comprobante").order("fecha_comprobante", desc=True).limit(1)
	for field, value in eq_filters.items():
		query = query.eq(field, value)
	for field, values in in_filters.items():
		if values:
			query = query.in_(field, values)
	result = query.execute()
	rows = result.data or []
	return _parse_iso_date(rows[0].get("fecha_comprobante")) if rows else None


def _month_sales_rows(sb: Client, scope: dict[str, object], latest_date: date) -> list[dict]:
	eq_filters, in_filters = _ventas_filters_for_scope(scope)
	return _fetch_all(
		sb,
		"ventas",
		"categoria,neto,kilos,pdv_codigo,fecha_comprobante",
		eq_filters=eq_filters,
		in_filters=in_filters,
		gte_filters={"fecha_comprobante": _first_day_of_month(latest_date).isoformat()},
		lte_filters={"fecha_comprobante": latest_date.isoformat()},
	)


def _day_sales_rows(sb: Client, scope: dict[str, object], target_date: date) -> list[dict]:
	eq_filters, in_filters = _ventas_filters_for_scope(scope)
	eq_filters["fecha_comprobante"] = target_date.isoformat()
	return _fetch_all(
		sb,
		"ventas",
		"categoria,neto,kilos,pdv_codigo",
		eq_filters=eq_filters,
		in_filters=in_filters,
	)


def _previous_period_rows(sb: Client, scope: dict[str, object], latest_date: date) -> list[dict]:
	previous_month = _month_shift(latest_date, -1)
	start = previous_month.replace(day=1)
	end = previous_month.replace(day=min(latest_date.day, calendar.monthrange(previous_month.year, previous_month.month)[1]))
	eq_filters, in_filters = _ventas_filters_for_scope(scope)
	return _fetch_all(
		sb,
		"ventas",
		"neto,kilos,pdv_codigo",
		eq_filters=eq_filters,
		in_filters=in_filters,
		gte_filters={"fecha_comprobante": start.isoformat()},
		lte_filters={"fecha_comprobante": end.isoformat()},
	)


def _holiday_set(sb: Client, latest_date: date) -> set[date]:
	rows = _fetch_all(
		sb,
		"feriados",
		"fecha",
		gte_filters={"fecha": _first_day_of_month(latest_date).isoformat()},
		lte_filters={"fecha": _last_day_of_month(latest_date).isoformat()},
	)
	return {parsed for row in rows if (parsed := _parse_iso_date(row.get("fecha")))}


def _config_token(sb: Client) -> str | None:
	row = sb.table("config").select("value,updated_at").eq("key", "ventas_ultima_actualizacion").maybe_single().execute().data
	if not row:
		return None
	value = row.get("value") or {}
	return value.get("timestamp") or row.get("updated_at")


def build_dashboard_dataset(sb: Client, user: UserContext, *, mes: int | None = None, anio: int | None = None) -> dict:
	scope = _get_scope(sb, user)

	# If specific month/year requested, find the last date within that month
	if mes is not None and anio is not None:
		target_end = _last_day_of_month(date(anio, mes, 1))
		target_start = date(anio, mes, 1)
		eq_filters, in_filters = _ventas_filters_for_scope(scope)
		rows = _fetch_all(
			sb, "ventas", "fecha_comprobante",
			eq_filters=eq_filters, in_filters=in_filters,
			gte_filters={"fecha_comprobante": target_start.isoformat()},
			lte_filters={"fecha_comprobante": target_end.isoformat()},
			order_by=("fecha_comprobante", True),
		)
		latest_date = _parse_iso_date(rows[0].get("fecha_comprobante")) if rows else None
	else:
		latest_date = _latest_sales_date(sb, scope)
	if latest_date is None:
		total_pdv = sb.table("pdv").select("id", count="exact", head=True).execute().count or 0
		return {
			"version": _config_token(sb),
			"latest_date": None,
			"business_days": {"elapsed": 0, "total": 0},
			"header": {
				"acumulado_neto": 0,
				"acumulado_kilos": 0,
				"tendencia_neto": 0,
				"tendencia_kilos": 0,
				"variacion_neto_pct": None,
				"variacion_kilos_pct": None,
				"cartera_activa": 0,
				"cobertura_pct": 0,
				"total_pdv_maestro": total_pdv,
				"pdvs_dia": 0,
			},
			"tabs": {"neto": [], "kilos": [], "clientes": []},
			"meta": {"uses_summary_for_kilos": False},
		}

	holidays = _holiday_set(sb, latest_date)
	elapsed_days = _business_days_between(_first_day_of_month(latest_date), latest_date, holidays)
	total_days = _business_days_between(_first_day_of_month(latest_date), _last_day_of_month(latest_date), holidays)

	summary_rows, kilos_from_summary = _summary_rows_for_month(sb, scope, latest_date)
	month_sales = _month_sales_rows(sb, scope, latest_date)
	prev_period_sales = _previous_period_rows(sb, scope, latest_date)
	day_7_rows = _day_sales_rows(sb, scope, latest_date - timedelta(days=7))
	day_14_rows = _day_sales_rows(sb, scope, latest_date - timedelta(days=14))
	today_rows = _day_sales_rows(sb, scope, latest_date)

	category_data: dict[str, dict] = defaultdict(
		lambda: {
			"categoria": "Sin categoría",
			"acumulado_neto": 0.0,
			"acumulado_kilos": 0.0,
			"pdvs_acumulados": 0,
			"mismo_dia_7_neto": 0.0,
			"mismo_dia_14_neto": 0.0,
			"mismo_dia_7_kilos": 0.0,
			"mismo_dia_14_kilos": 0.0,
			"pdvs_hoy": set(),
		}
	)

	for row in summary_rows:
		categoria = row.get("categoria") or "Sin categoría"
		category = category_data[categoria]
		category["categoria"] = categoria
		category["acumulado_neto"] += _safe_float(row.get("total_neto"))
		category["pdvs_acumulados"] += int(row.get("pdv_activos") or 0)
		if kilos_from_summary:
			category["acumulado_kilos"] += _safe_float(row.get("total_kilos"))

	if not kilos_from_summary:
		for row in month_sales:
			categoria = row.get("categoria") or "Sin categoría"
			category = category_data[categoria]
			category["categoria"] = categoria
			category["acumulado_kilos"] += _safe_float(row.get("kilos"))

	for row in day_7_rows:
		categoria = row.get("categoria") or "Sin categoría"
		category = category_data[categoria]
		category["categoria"] = categoria
		category["mismo_dia_7_neto"] += _safe_float(row.get("neto"))
		category["mismo_dia_7_kilos"] += _safe_float(row.get("kilos"))

	for row in day_14_rows:
		categoria = row.get("categoria") or "Sin categoría"
		category = category_data[categoria]
		category["categoria"] = categoria
		category["mismo_dia_14_neto"] += _safe_float(row.get("neto"))
		category["mismo_dia_14_kilos"] += _safe_float(row.get("kilos"))

	pdvs_hoy_total: set[str] = set()
	for row in today_rows:
		categoria = row.get("categoria") or "Sin categoría"
		category = category_data[categoria]
		category["categoria"] = categoria
		pdv_codigo = row.get("pdv_codigo")
		if pdv_codigo:
			category["pdvs_hoy"].add(pdv_codigo)
			pdvs_hoy_total.add(pdv_codigo)

	total_pdv_query = sb.table("pdv").select("id", count="exact", head=True)
	if scope.get("carteras"):
		total_pdv_query = total_pdv_query.in_("cartera", scope["carteras"])
	total_pdv_maestro = total_pdv_query.execute().count or 0

	cartera_activa = len({row.get("pdv_codigo") for row in month_sales if row.get("pdv_codigo")})
	prev_neto = sum(_safe_float(row.get("neto")) for row in prev_period_sales)
	prev_kilos = sum(_safe_float(row.get("kilos")) for row in prev_period_sales)

	total_neto = sum(row["acumulado_neto"] for row in category_data.values())
	total_kilos = sum(row["acumulado_kilos"] for row in category_data.values())
	media_neto = total_neto / elapsed_days if elapsed_days else 0
	media_kilos = total_kilos / elapsed_days if elapsed_days else 0

	neto_rows = []
	kilos_rows = []
	clientes_rows = []
	for category in category_data.values():
		media_real_neto = category["acumulado_neto"] / elapsed_days if elapsed_days else 0
		media_real_kilos = category["acumulado_kilos"] / elapsed_days if elapsed_days else 0
		pdvs_hoy_count = len(category["pdvs_hoy"])
		clientes_rows.append(
			{
				"categoria": category["categoria"],
				"pdvs_acumulados": category["pdvs_acumulados"],
				"pdvs_hoy": pdvs_hoy_count,
				"sin_concretar": max(len(pdvs_hoy_total) - pdvs_hoy_count, 0),
				"pct_pdvs_dia": (pdvs_hoy_count / len(pdvs_hoy_total) * 100) if pdvs_hoy_total else 0,
			}
		)
		neto_rows.append(
			{
				"categoria": category["categoria"],
				"acumulado": round(category["acumulado_neto"], 2),
				"tendencia": round(media_real_neto * total_days, 2),
				"media_real": round(media_real_neto, 2),
				"mismo_dia_7": round(category["mismo_dia_7_neto"], 2),
				"mismo_dia_14": round(category["mismo_dia_14_neto"], 2),
			}
		)
		kilos_rows.append(
			{
				"categoria": category["categoria"],
				"acumulado": round(category["acumulado_kilos"], 3),
				"tendencia": round(media_real_kilos * total_days, 3),
				"media_real": round(media_real_kilos, 3),
				"mismo_dia_7": round(category["mismo_dia_7_kilos"], 3),
				"mismo_dia_14": round(category["mismo_dia_14_kilos"], 3),
			}
		)

	neto_rows.sort(key=lambda row: row["acumulado"], reverse=True)
	kilos_rows.sort(key=lambda row: row["acumulado"], reverse=True)
	clientes_rows.sort(key=lambda row: row["pdvs_acumulados"], reverse=True)

	return {
		"version": _config_token(sb),
		"latest_date": latest_date.isoformat(),
		"business_days": {"elapsed": elapsed_days, "total": total_days},
		"header": {
			"acumulado_neto": round(total_neto, 2),
			"acumulado_kilos": round(total_kilos, 3),
			"tendencia_neto": round(media_neto * total_days, 2),
			"tendencia_kilos": round(media_kilos * total_days, 3),
			"variacion_neto_pct": round(((total_neto / prev_neto) - 1) * 100, 2) if prev_neto else None,
			"variacion_kilos_pct": round(((total_kilos / prev_kilos) - 1) * 100, 2) if prev_kilos else None,
			"cartera_activa": cartera_activa,
			"cobertura_pct": round((cartera_activa / total_pdv_maestro) * 100, 2) if total_pdv_maestro else 0,
			"total_pdv_maestro": total_pdv_maestro,
			"pdvs_dia": len(pdvs_hoy_total),
		},
		"tabs": {
			"neto": neto_rows,
			"kilos": kilos_rows,
			"clientes": clientes_rows,
		},
		"meta": {"uses_summary_for_kilos": kilos_from_summary},
	}


def get_dashboard_version(sb: Client) -> dict[str, str | None]:
	return {"version": _config_token(sb)}


def get_available_periods(sb: Client, user: UserContext) -> list[dict]:
	"""Return distinct year/month pairs that have sales data, sorted desc.

	Since PostgREST doesn't support DISTINCT, we probe year/month combos
	by checking a small limit=1 query per candidate. Fast because each
	query returns immediately and there are at most ~24 combos to check.
	"""
	# Get the range of years present
	scope = _get_scope(sb, user)
	eq_filters, in_filters = _ventas_filters_for_scope(scope)

	# Newest row
	query = sb.table("ventas").select("anio,mes").order("fecha_comprobante", desc=True).limit(1)
	for k, v in eq_filters.items():
		query = query.eq(k, v)
	for k, vs in in_filters.items():
		if vs:
			query = query.in_(k, vs)
	newest_rows = (query.execute()).data or []
	if not newest_rows:
		return []
	max_anio = int(newest_rows[0].get("anio") or 0)
	max_mes = int(newest_rows[0].get("mes") or 0)

	# Oldest row
	query2 = sb.table("ventas").select("anio,mes").order("fecha_comprobante", desc=False).limit(1)
	for k, v in eq_filters.items():
		query2 = query2.eq(k, v)
	for k, vs in in_filters.items():
		if vs:
			query2 = query2.in_(k, vs)
	oldest_rows = (query2.execute()).data or []
	min_anio = int(oldest_rows[0].get("anio") or max_anio) if oldest_rows else max_anio
	min_mes = int(oldest_rows[0].get("mes") or 1) if oldest_rows else 1

	# Probe each year/month combo with limit=1
	periods = []
	for year in range(max_anio, min_anio - 1, -1):
		m_start = max_mes if year == max_anio else 12
		m_end = min_mes if year == min_anio else 1
		for month in range(m_start, m_end - 1, -1):
			ef = {**eq_filters, "anio": year, "mes": month}
			try:
				check = sb.table("ventas").select("id", count="exact", head=True)
				for k, v in ef.items():
					check = check.eq(k, v)
				for k, vs in in_filters.items():
					if vs:
						check = check.in_(k, vs)
				result = check.execute()
				if (result.count or 0) > 0:
					periods.append({"anio": year, "mes": month})
			except Exception:
				pass
	return periods
