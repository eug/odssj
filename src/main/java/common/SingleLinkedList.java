package common;

import java.util.Iterator;
import java.util.Objects;

public class SingleLinkedList<T> implements Iterable<T> {
	
	private class Node {
		public final T value;
		public Node next;
		public Node(T value) {
			this.value = value;
			this.next = null;
		}
	}
	
	private class SingleLinkedListIterator<T> implements Iterator<T> {
		
		SingleLinkedList<T>.Node current;
		
		public SingleLinkedListIterator(SingleLinkedList<T> sll) {
			current = sll.head;
		}
		
		@Override
		public boolean hasNext() {
			return !Objects.isNull(current);
		}

		@Override
		public T next() {
			T value = current.value;
			current = current.next;
			return value;
		}
	}
	
	private Node head;
	private Node tail;
	private Node current;
	private int size;
	
	public void reset() {
		current = head;
	}
	
	public void skip(int pos) {
		if ((pos > 0) && (pos < size))
			for (; pos > 0; pos--)
				current = current.next;
	}
	public T getCurrent() {
		return current.value;
	}
	public void add(T value) {
		if (Objects.isNull(value))
			return;
					
		Node node = new Node(value);
		
		if (Objects.isNull(head)) { 
			head = node;
			tail = node;
			current = node;
		} else {
			tail.next = node;
			tail = node;
		}
		
		size++;
	}
	
	public void add(SingleLinkedList<T> sll) {
		if (Objects.isNull(sll))
			return;
		
		tail.next = sll.head;
		tail = sll.tail;
		size += sll.size();
	}
	
	public int size() {
		return size;
	}

	@Override
	public Iterator<T> iterator() {
		return new SingleLinkedListIterator<>(this);
	}
}
