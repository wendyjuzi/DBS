from __future__ import annotations
from typing import Any, List, Optional, Tuple


class BPlusTreeNode:
	def __init__(self, order: int, leaf: bool = False):
		self.order = order
		self.leaf = leaf
		self.keys: List[str] = []
		# 对于叶子：children[i] 存放同键的值列表(List[str])；
		# 对于内部：children[i] 指向子节点
		self.children: List[Any] = []
		# 叶子链表
		self.next: Optional[BPlusTreeNode] = None

	def split(self) -> Tuple[str, 'BPlusTreeNode']:
		mid = len(self.keys) // 2
		if self.leaf:
			# 叶子分裂：中间及右侧上移一个分裂键（右侧第一个）
			right = BPlusTreeNode(self.order, leaf=True)
			right.keys = self.keys[mid:]
			right.children = self.children[mid:]
			right.next = self.next
			self.keys = self.keys[:mid]
			self.children = self.children[:mid]
			self.next = right
			promote_key = right.keys[0]
			return promote_key, right
		else:
			# 内部节点分裂：中位键上移，其右侧给新节点
			right = BPlusTreeNode(self.order, leaf=False)
			promote_key = self.keys[mid]
			right.keys = self.keys[mid+1:]
			right.children = self.children[mid+1:]
			self.keys = self.keys[:mid]
			self.children = self.children[:mid+1]
			return promote_key, right


class BPlusTree:
	def __init__(self, order: int = 32):
		self.order = max(4, order)
		self.root = BPlusTreeNode(self.order, leaf=True)

	def _find_leaf(self, key: str) -> BPlusTreeNode:
		n = self.root
		while not n.leaf:
			i = 0
			while i < len(n.keys) and key >= n.keys[i]:
				i += 1
			n = n.children[i]
		return n

	def insert(self, key: str, value: str) -> None:
		leaf = self._find_leaf(key)
		# 插入到叶子合适位置（多值）
		i = 0
		while i < len(leaf.keys) and key > leaf.keys[i]:
			i += 1
		if i < len(leaf.keys) and leaf.keys[i] == key:
			lst: List[str] = leaf.children[i]
			if value not in lst:
				lst.append(value)
		else:
			leaf.keys.insert(i, key)
			leaf.children.insert(i, [value])
		# 分裂
		self._rebalance_after_insert(leaf)

	def _rebalance_after_insert(self, node: BPlusTreeNode) -> None:
		while len(node.keys) >= self.order:
			promote, right = node.split()
			if node is self.root:
				new_root = BPlusTreeNode(self.order, leaf=False)
				new_root.keys = [promote]
				new_root.children = [node, right]
				self.root = new_root
				return
			# 找到父节点并插入（简单实现：自根向下查找一次）
			parent = self._find_parent(self.root, node)
			if parent is None:
				return
			i = 0
			while i < len(parent.keys) and promote >= parent.keys[i]:
				i += 1
			parent.keys.insert(i, promote)
			parent.children.insert(i+1, right)
			node = parent

	def _find_parent(self, cur: BPlusTreeNode, child: BPlusTreeNode) -> Optional[BPlusTreeNode]:
		if cur.leaf:
			return None
		for c in cur.children:
			if c is child:
				return cur
			p = self._find_parent(c, child)
			if p is not None:
				return p
		return None

	def get(self, key: str) -> List[str]:
		leaf = self._find_leaf(key)
		for i, k in enumerate(leaf.keys):
			if k == key:
				return list(leaf.children[i])
		return []

	def delete_value(self, key: str, value: str) -> None:
		leaf = self._find_leaf(key)
		for i, k in enumerate(leaf.keys):
			if k == key:
				lst = leaf.children[i]
				if value in lst:
					lst.remove(value)
					if not lst:
						leaf.keys.pop(i)
						leaf.children.pop(i)
				return

	def range(self, min_key: Optional[str] = None, max_key: Optional[str] = None,
	          include_min: bool = True, include_max: bool = True) -> List[str]:
		# 从最左叶子或min对应叶子开始，线性遍历叶链
		start = self.root if self.root.leaf else (self._find_leaf(min_key or "") if min_key is not None else self._leftmost())
		out: List[str] = []
		n = start
		while n:
			for i, k in enumerate(n.keys):
				if min_key is not None:
					if k < min_key or (k == min_key and not include_min):
						continue
				if max_key is not None:
					if k > max_key or (k == max_key and not include_max):
						return out
				out.extend(n.children[i])
			n = n.next
		return out

	def _leftmost(self) -> BPlusTreeNode:
		n = self.root
		while not n.leaf:
			n = n.children[0]
		return n


