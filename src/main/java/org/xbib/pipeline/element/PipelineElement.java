package org.xbib.pipeline.element;

public interface PipelineElement<E> {

    E get();

    PipelineElement<E> set(E e);

}
