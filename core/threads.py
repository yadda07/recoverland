"""Background threads for RecoverLand plugin."""
from qgis.PyQt.QtCore import pyqtSignal, QThread
from datetime import datetime
import traceback
import gc
import random
from typing import Optional, Dict, List

from .constants import THREAD_STOP_TIMEOUT, psycopg2
from .logger import flog, LoggerMixin
from .database import DatabaseMixin


class RecoverThread(QThread, LoggerMixin, DatabaseMixin):
    """Thread for async data recovery from audit tables."""
    
    progress_updated = pyqtSignal(int)
    phase_changed = pyqtSignal(str)
    process_complete = pyqtSignal(bool, object, int)
    error_occurred = pyqtSignal(str)
    log_message = pyqtSignal(str, int)
    
    def __init__(self, db_params: Dict[str, str], schema: str, table: str, 
                 operation: str, start_time: str, end_time: str, 
                 user_filter: Optional[str] = None):
        QThread.__init__(self)
        self.db_params = db_params
        self.schema = schema
        self.table = table
        self.operation = operation
        self.start_time = start_time
        self.end_time = end_time
        self.user_filter = user_filter if user_filter != 'ALL' else None
        self.is_running = False
        self._conn = None
        
    def run(self) -> None:
        """Execute recovery in background thread."""
        flog(f"RecoverThread.run: START schema={self.schema} table={self.table} op={self.operation}")
        flog(f"RecoverThread.run: period={self.start_time} -> {self.end_time} filter={self.user_filter}")
        self.is_running = True
        conn = None
        cur = None
        
        try:
            temp_table_name = f"temp_{self.schema}_{self.table}"
            flog(f"RecoverThread.run: temp_table_name={temp_table_name}")
            
            # Generate random segment boundaries so progress never pauses at the same spots
            seg1_end = random.randint(7, 14)
            seg2_end = random.randint(seg1_end + 5, seg1_end + 12)
            seg3_end = random.randint(seg2_end + 15, seg2_end + 28)
            seg4_end = random.randint(seg3_end + 8, seg3_end + 18)
            seg5_end = min(random.randint(seg4_end + 4, seg4_end + 12), 88)
            seg6_end = random.randint(max(seg5_end + 3, 89), 97)
            
            self.phase_changed.emit("Connexion à la base de données...")
            # Smooth progress -> seg1_end
            for i in range(1, seg1_end + 1):
                if not self.is_running:
                    flog("RecoverThread.run: cancelled before connect")
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(15, 45))
            
            # Connect to DB
            flog("RecoverThread.run: connecting to DB...")
            conn = self.create_connection(self.db_params, "Recovery")
            self._conn = conn
            conn.autocommit = True
            cur = conn.cursor()
            self.set_statement_timeout(cur)
            flog("RecoverThread.run: DB connected, timeout set")
            
            # Smooth progress -> seg2_end
            for i in range(seg1_end + 1, seg2_end + 1):
                if not self.is_running:
                    flog("RecoverThread.run: cancelled after connect")
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(10, 35))
            
            flog(f"RecoverThread.run: user_filter='{self.user_filter}'")
            
            self.phase_changed.emit("Exécution de la requête de récupération...")
            # Appel fonction SQL (6 params)
            flog("RecoverThread.run: calling rip_avg_nge.recover()...")
            start_recover_time = datetime.now()
            cur.execute("""
                SELECT rip_avg_nge.recover(
                    %s::text, 
                    %s::text, 
                    %s::character varying, 
                    %s::timestamp, 
                    %s::timestamp, 
                    NULL::integer
                );
            """, (self.schema, self.table, self.operation, 
                  self.start_time, self.end_time))
            
            recover_duration = (datetime.now() - start_recover_time).total_seconds()
            flog(f"RecoverThread.run: recover() done in {recover_duration:.2f}s")
            
            # Smooth progress -> seg3_end
            for i in range(seg2_end + 1, seg3_end + 1):
                if not self.is_running:
                    flog("RecoverThread.run: cancelled after recover()")
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(15, 45))
            
            self.phase_changed.emit("Comptage des résultats...")
            flog(f"RecoverThread.run: counting from {temp_table_name}...")
            
            # Smooth progress -> seg4_end
            for i in range(seg3_end + 1, seg4_end + 1):
                if not self.is_running:
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(12, 40))
            
            # Count avec filtre utilisateur si spécifié
            from psycopg2 import sql
            if self.user_filter:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE user_name = %s").format(
                    sql.Identifier(temp_table_name)), (self.user_filter,))
            else:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(temp_table_name)))
            count = cur.fetchone()[0]
            flog(f"RecoverThread.run: count={count}")
            
            # Smooth progress -> seg5_end
            for i in range(seg4_end + 1, seg5_end + 1):
                if not self.is_running:
                    flog("RecoverThread.run: cancelled after count")
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(10, 30))
            
            if count == 0:
                self.log_info(f"Aucune entité trouvée pour {self.schema}.{self.table} ({self.operation})")
                self.process_complete.emit(False, None, 0)
                return
            
            # Copier vers table persistante dans schéma 'temporaire'
            username = self.db_params["user"]
            date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            persist_table = f"temp_{username}_{self.schema}_{self.table}_{self.operation.lower()}_{date_suffix}"
            
            self.phase_changed.emit("Sauvegarde des résultats...")
            # Transaction atomique pour DROP+CREATE (évite perte si CREATE échoue)
            flog(f"RecoverThread.run: persist_table={persist_table}")
            flog("RecoverThread.run: switching autocommit=False for DROP+CREATE")
            conn.autocommit = False
            flog("RecoverThread.run: autocommit switched OK")
            try:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier('temporaire', persist_table)))
                
                if self.user_filter:
                    cur.execute(sql.SQL("CREATE TABLE {} AS SELECT * FROM {} WHERE user_name = %s").format(
                        sql.Identifier('temporaire', persist_table),
                        sql.Identifier(temp_table_name)), (self.user_filter,))
                else:
                    cur.execute(sql.SQL("CREATE TABLE {} AS SELECT * FROM {}").format(
                        sql.Identifier('temporaire', persist_table),
                        sql.Identifier(temp_table_name)))
                
                flog("RecoverThread.run: committing DROP+CREATE...")
                conn.commit()
                flog("RecoverThread.run: DROP+CREATE committed OK")
            except Exception as persist_err:
                flog(f"RecoverThread.run: DROP+CREATE FAILED: {persist_err}", "ERROR")
                conn.rollback()
                raise
            
            # Smooth progress -> seg6_end
            for i in range(seg5_end + 1, seg6_end + 1):
                if not self.is_running:
                    flog("RecoverThread.run: cancelled after persist")
                    return
                self.progress_updated.emit(i)
                QThread.msleep(random.randint(10, 30))
            
            self.phase_changed.emit("Chargement de la couche...")
            self._emit_log(f"Table persistante: temporaire.{persist_table}", 0)
            
            layer_info = {
                "host": self.db_params["host"],
                "port": self.db_params["port"],
                "dbname": self.db_params["dbname"],
                "user": self.db_params["user"],
                "schema": "temporaire",
                "table": persist_table,
                "layer_name": f"{self.table} ({self.operation}) - {self.start_time[:10]} au {self.end_time[:10]}"
            }
            
            flog(f"RecoverThread.run: SUCCESS {count} entities recovered")
            self.log_info(f"Récupération terminée: {count} entités récupérées")
            self.process_complete.emit(True, layer_info, count)
            
        except Exception as e:
            flog(f"RecoverThread.run: EXCEPTION: {e}", "ERROR")
            flog(traceback.format_exc(), "ERROR")
            self.error_occurred.emit(str(e))
            self.process_complete.emit(False, None, 0)
        
        finally:
            flog("RecoverThread.run: FINALLY block entered")
            self._conn = None
            
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
                flog("RecoverThread.run: DB connection closed")
            except Exception as close_err:
                flog(f"RecoverThread.run: close error: {close_err}", "WARNING")
                
            self.is_running = False
            gc.collect()
            flog("RecoverThread.run: END")
            
    def stop(self) -> None:
        """Gracefully stop thread without terminate()."""
        if self.is_running:
            self.is_running = False
            try:
                if self._conn:
                    self._conn.cancel()
            except Exception:
                pass
            if not self.wait(THREAD_STOP_TIMEOUT):
                self._emit_log("Thread non terminé dans le délai imparti", 1)
    
    def _emit_log(self, message: str, level: int) -> None:
        """Emit log via signal for thread-safe logging."""
        self.log_message.emit(message, level)


class RestoreThread(QThread, LoggerMixin, DatabaseMixin):
    """Thread for async data restoration to source tables."""
    
    progress_updated = pyqtSignal(int)
    restore_complete = pyqtSignal(bool, str, int)
    error_occurred = pyqtSignal(str)
    log_message = pyqtSignal(str, int)
    
    def __init__(self, db_params: Dict[str, str], schema: str, table: str, 
                 selected_gids: List, operation: Optional[str] = None, 
                 restore_data_rows: Optional[List] = None, 
                 temp_table_name: Optional[str] = None, 
                 temp_row_ids: Optional[List] = None):
        QThread.__init__(self)
        self.db_params = db_params
        self.schema = schema
        self.table = table
        self.selected_gids = selected_gids
        self.operation = operation
        self.restore_data_rows = restore_data_rows or []
        self.temp_table_name = temp_table_name
        self.temp_row_ids = temp_row_ids or []
        self.is_running = False
        self._conn = None
        
    def run(self) -> None:
        """Execute restoration in background thread."""
        self.is_running = True
        conn = None
        cur = None
        
        try:
            from psycopg2 import sql
            self.progress_updated.emit(10)
            conn = self.create_connection(self.db_params, "Restore")
            self._conn = conn
            conn.autocommit = False
            
            cur = conn.cursor()
            self.set_statement_timeout(cur)
            
            self.progress_updated.emit(30)
            
            if not self.temp_table_name or not self.temp_row_ids:
                raise ValueError("Informations de table temporaire manquantes.")
            
            self._emit_log(f"RestoreThread: {len(self.temp_row_ids)} à restaurer", 0)
            
            self.progress_updated.emit(40)
            
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                AND column_name NOT IN ('user_name', 'audit_timestamp')
                ORDER BY ordinal_position
            """, (self.schema, self.table))
            
            columns = [row[0] for row in cur.fetchall()]
            
            self.progress_updated.emit(50)
            self._emit_log(f"Opération: {self.operation}", 0)
            
            restored_count = 0
            
            # Parse temp table name for sql.Identifier
            temp_parts = self.temp_table_name.split('.')
            temp_schema_name = temp_parts[0] if len(temp_parts) > 1 else 'temporaire'
            temp_table_name = temp_parts[-1]
            
            total = len(self.temp_row_ids)
            
            if self.operation == 'DELETE':
                self._emit_log("Restauration DELETE: réinsertion", 0)
                
                col_ids = sql.SQL(',').join([sql.Identifier(c) for c in columns])
                
                for idx, temp_id in enumerate(self.temp_row_ids):
                    if not self.is_running:
                        break
                    parts = temp_id.split('_')
                    gid_value = parts[0]
                    timestamp_parts = '_'.join(parts[1:]).replace('_', '-', 2).replace('_', ' ', 1).replace('_', ':')
                    savepoint_name = f"sp_{gid_value}".replace('.', '_')
                    
                    cur.execute(sql.SQL("SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                    
                    try:
                        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE gid = %s").format(
                            sql.Identifier(self.schema), sql.Identifier(self.table)), (gid_value,))
                        exists_in_target = cur.fetchone()[0] > 0
                        
                        timestamp_like = f"{timestamp_parts}%"
                        
                        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE gid = %s AND audit_timestamp::text LIKE %s").format(
                            sql.Identifier(temp_schema_name), sql.Identifier(temp_table_name)), 
                            (gid_value, timestamp_like))
                        exists_in_temp = cur.fetchone()[0] > 0
                        
                        if not exists_in_temp:
                            self._emit_log(f"GID {gid_value} non trouvé dans temp", 1)
                        elif not exists_in_target:
                            insert_query = sql.SQL("INSERT INTO {}.{} ({}) SELECT {} FROM {}.{} WHERE gid = %s AND audit_timestamp::text LIKE %s").format(
                                sql.Identifier(self.schema), sql.Identifier(self.table),
                                col_ids, col_ids,
                                sql.Identifier(temp_schema_name), sql.Identifier(temp_table_name))
                            
                            cur.execute(insert_query, (gid_value, timestamp_like))
                            if cur.rowcount > 0:
                                restored_count += 1
                                self._emit_log(f"GID {gid_value} restauré", 0)
                        else:
                            self._emit_log(f"GID {gid_value} existe déjà", 1)
                        
                        cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                        
                    except psycopg2.Error as e:
                        cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                        self._emit_log(f"Erreur GID {gid_value}: {e}", 1)
                    
                    self.progress_updated.emit(50 + int(40 * (idx + 1) / total))
            
            elif self.operation == 'UPDATE':
                self._emit_log(f"Restauration UPDATE: {len(self.temp_row_ids)} entités", 0)
                update_columns = [col for col in columns if col != 'gid']
                
                set_clause = sql.SQL(',').join([
                    sql.SQL("{} = temp.{}").format(sql.Identifier(c), sql.Identifier(c)) 
                    for c in update_columns
                ])
                
                for idx, temp_id in enumerate(self.temp_row_ids):
                    if not self.is_running:
                        break
                    parts = temp_id.split('_')
                    gid_value = parts[0]
                    timestamp_parts = '_'.join(parts[1:]).replace('_', '-', 2).replace('_', ' ', 1).replace('_', ':')
                    savepoint_name = f"sp_upd_{gid_value}".replace('.', '_')
                    
                    cur.execute(sql.SQL("SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                    
                    try:
                        timestamp_like = f"{timestamp_parts}%"
                        
                        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE gid = %s AND audit_timestamp::text LIKE %s").format(
                            sql.Identifier(temp_schema_name), sql.Identifier(temp_table_name)), 
                            (gid_value, timestamp_like))
                        exists_in_temp = cur.fetchone()[0] > 0
                        
                        if not exists_in_temp:
                            self._emit_log(f"GID {gid_value} non trouvé dans temp", 1)
                            cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                            continue
                        
                        update_query = sql.SQL("""
                            UPDATE {schema}.{table} 
                            SET {sets}
                            FROM {temp_schema}.{temp_table} temp
                            WHERE {schema}.{table}.gid = temp.gid 
                              AND temp.gid = %s
                              AND temp.audit_timestamp::text LIKE %s
                        """).format(
                            schema=sql.Identifier(self.schema),
                            table=sql.Identifier(self.table),
                            sets=set_clause,
                            temp_schema=sql.Identifier(temp_schema_name),
                            temp_table=sql.Identifier(temp_table_name)
                        )
                        
                        cur.execute(update_query, (gid_value, timestamp_like))
                        if cur.rowcount > 0:
                            restored_count += 1
                            self._emit_log(f"GID {gid_value} mis à jour", 0)
                        else:
                            self._emit_log(f"GID {gid_value}: pas de modification", 1)
                        
                        cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                        
                    except psycopg2.Error as e:
                        cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
                        self._emit_log(f"Erreur UPDATE GID {gid_value}: {e}", 1)
                    
                    self.progress_updated.emit(50 + int(40 * (idx + 1) / total))
            
            else:
                self._emit_log(f"Opération '{self.operation}' non implémentée", 1)
            
            conn.commit()
            self.progress_updated.emit(90)
            
            self._emit_log(f"Restauration terminée: {restored_count} entités", 0)
            self.restore_complete.emit(True, f"{restored_count} entités restaurées", restored_count)
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.error_occurred.emit(str(e))
            self.restore_complete.emit(False, str(e), 0)
        
        finally:
            self._conn = None
            
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            
            self.is_running = False
            gc.collect()
    
    def stop(self) -> None:
        """Gracefully stop thread without terminate()."""
        if self.is_running:
            self.is_running = False
            try:
                if self._conn:
                    self._conn.cancel()
            except Exception:
                pass
            if not self.wait(THREAD_STOP_TIMEOUT):
                self._emit_log("Thread non terminé dans le délai", 1)
    
    def _emit_log(self, message: str, level: int) -> None:
        """Emit log via signal for thread-safe logging."""
        self.log_message.emit(message, level)
