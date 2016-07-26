/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

/**
 * An array backed auto-scalable holdable pool. 
 * The pool will be empty until the first time getHold(...) is called.
 * When all of the objects in the pool are currently held, call to getHold(...) will result either a new object being created if the size limit has not been reached,
 * or an infinite loop until an object is released and can be held.
 * getHold(...) method of this class will never return null. 
 * @author James Hu
 *
 */
public class ArrayScalableHoldablePool<T> implements HoldablePool<T> {
	protected Supplier<T> factory;
	protected AtomicReferenceArray<Holdable<T>> pool;
	
	/**
	 * Constructor
	 * @param factory	the factory function to create the objects
	 * @param size		the maximum number of objects allowed to be craeted
	 */
	public ArrayScalableHoldablePool(Supplier<T> factory, int size){
		this.factory = factory;
		this.pool = new AtomicReferenceArray<>(size);
	}
	
	/**
	 * Constructor. The size of the pool will be set to the same as the number of CPU cores.
	 * @param factory	the factory function to create the objects
	 */
	public ArrayScalableHoldablePool(Supplier<T> factory){
		this(factory, Runtime.getRuntime().availableProcessors());
	}

	@Override
	public Holdable<T> getHold(long holdId) {
		for (int i = 0; ; i = (i+1) % pool.length()){
			Holdable<T> holdable = pool.get(i);
			if (holdable == null){
				Holdable<T> newHoldable = new Holdable<>(factory.get());
				holdable = pool.updateAndGet(i, v -> v == null ? newHoldable : v);
			}
			if (holdable.hold(holdId)){
				return holdable;
			}
		}
	}

	@Override
	public Collection<T> getAll() {
		List<T> result = new ArrayList<>();
		for (int i = 0; i < pool.length(); i ++){
			Holdable<T> holdable = pool.get(i);
			if (holdable != null){
				result.add(holdable.get());
			}
		}
		return result;
	}

	@Override
	public void reset(T object) {
		for (int i = 0; i < pool.length(); i ++){
			pool.set(i, null);
		}
		
		if (object != null){
			pool.set(0, new Holdable<>(object));
		}
		
	}

	@Override
	public int getSize() {
		for (int i = 0; i < pool.length(); i ++){
			if (pool.get(i) == null){
				return i;
			}
		}
		return pool.length();
	}

	@Override
	public int getCapacity() {
		return pool.length();
	}


}
