#!/usr/bin/python
import time
from os import path, listdir
from sys import stderr
from shutil import copy2
from Reddit import Comment, Post

try:                import sqlite3
except ImportError: import sqlite as sqlite3

SCHEMA = {
	'posts' :
		'id        text primary key,' +
		'shorturl  text,' +
		'url       text,' +
		'subreddit text,' +
		'author    text,' +
		'title     text',
		
	'config' :
		'key   text primary key,' +
		'value text',
}

DB_FILE = 'posts.db'

class DB:
	def __init__(self):
		self.logger = stderr
		if path.exists(DB_FILE):
			self.debug('__init__: using database file: %s' % DB_FILE)
		else:
			self.debug('__init__: database file (%s) not found, creating...' % DB_FILE)
		self.conn = None
		self.conn = sqlite3.connect(DB_FILE) #TODO CHANGE BACK, encoding='utf-8')
		self.conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
		# Don't create tables if not supplied.
		if SCHEMA != None and SCHEMA != {} and len(SCHEMA) > 0:
			# Create table for every schema given.
			for key in SCHEMA:
				self.create_table(key, SCHEMA[key])
	
	def debug(self, text):
		tstamp = time.strftime('[%Y-%m-%dT%H:%M:%SZ]', time.gmtime())
		text = '%s DB: %s' % (tstamp, text)
		self.logger.write('%s\n' % text)
		if self.logger != stderr:
			stderr.write('%s\n' % text)
	
	def create_table(self, table_name, schema):
		cur = self.conn.cursor()
		query = '''create table if not exists %s (%s)''' % (table_name, schema)
		cur.execute(query)
		self.commit()
		cur.close()
	
	def commit(self):
		try_again = True
		while try_again:
			try:
				self.conn.commit()
				try_again = False
			except:
				time.sleep(1)
	
	def insert(self, table, values):
		cur = self.conn.cursor()
		try:
			questions = ''
			for i in xrange(0, len(values)):
				if questions != '': questions += ','
				questions += '?'
			exec_string = '''insert into %s values (%s)''' % (table, questions)
			result = cur.execute(exec_string, values)
			last_row_id = cur.lastrowid
			cur.close()
			return last_row_id
		except sqlite3.IntegrityError:
			cur.close()
			return -1
	
	def delete(self, table, where, values=[]):
		cur = self.conn.cursor()
		q = '''
			delete from %s
				where %s
		''' % (table, where)
		cur.execute(q, values)
	
	def get_cursor(self):
		return self.conn.cursor()
	
	def count(self, table, where, values=[]):
		return self.select_one('count(*)', table, where, values=values)
	
	def select(self, what, table, where='', values=[]):
		cur = self.conn.cursor()
		query = '''
			select %s
				from %s
		''' % (what, table)
		if where != '':
			query += 'where %s' % (where)
		cur.execute(query, values)
		results = []
		for result in cur:
			results.append(result)
		cur.close()
		return results

	def select_one(self, what, table, where='', values=[]):
		cur = self.conn.cursor()
		if where != '':
			where = 'where %s' % where
		query = '''
			select %s
				from %s
				%s
		''' % (what, table, where)
		execur = cur.execute(query, values)
		one = execur.fetchone()
		cur.close()
		return one[0]
	
	def execute(self, statement):
		cur = self.conn.cursor()
		result = cur.execute(statement)
		return result

	def get_config(self, key):
		cur = self.conn.cursor()
		query = '''
			select value
				from config
				where key = "%s"
		''' % key
		try:
			execur = cur.execute(query)
			result = execur.fetchone()[0]
			cur.close()
		except Exception, e:
			self.debug('failed to get config key "%s": %s' % (key, str(e)))
			return None
		return result

	def set_config(self, key, value):
		cur = self.conn.cursor()
		query = '''
			insert or replace into config (key, value)
				values ("%s", "%s")
		''' % (key, value)
		try:
			execur = cur.execute(query)
			result = execur.fetchone()
			self.commit()
			cur.close()
		except Exception, e:
			self.debug('failed to set config key "%s" to value "%s": %s' % (key, value, str(e)))
		

if __name__ == '__main__':
	db = DB()
	try: db.add_user('4_pr0n')
	except: pass
	db.set_last_since_id('4_pr0n', 'ccs4ule')
	print db.get_last_since_id('4_pr0n')
