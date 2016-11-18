package no.ssb.jsonstat.v2;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import me.yanaga.guava.stream.MoreCollectors;
import no.ssb.jsonstat.JsonStat;

import java.time.Instant;
import java.util.*;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A model of the JSON-stat dataset format.
 * <p>
 * This model is a java based implementation of the JSON-stat format defined at
 * <a href="https://json-stat.org/">json-stat.org/</a>. It relies heavily on Java 8 and the Google Guava library.
 * <p>
 * Instances of this class are immutable and must be created using the provided {@link Dataset#create(String)} static
 * method.
 */
public abstract class Dataset extends JsonStat {

    private final String label;
    private final String source;
    private final Instant updated;
    // TODO: Support for status.

    protected Dataset(String label, String source, Instant updated) {
        super(Version.TWO, Class.DATASET);
        this.label = label;
        this.source = source;
        this.updated = updated;
    }

    /**
     * Create a new {@link Builder} instance.
     */
    public static DatasetBuilder create() {
        return new Builder();
    }

    /**
     * Create a new {@link Builder} instance.
     */
    public static DatasetBuilder create(String label) {
        Builder builder = new Builder();
        return builder.withLabel(label);
    }

    /**
     * Return an {@link ImmutableSet} with the available dimensions in
     * the dataset, in order. It is consistent with {@link #getSize()}.
     *
     * @see <a href="https://json-stat.org/format/#id">json-stat.org/format/#id</a>
     */
    public ImmutableSet<String> getId() {
        return ImmutableSet.copyOf(getDimension().keySet());
    }

    /**
     * Return an {@link ImmutableList} with the size of the available dimensions in
     * the dataset, in order. It is consistent with {@link #getId()}.
     *
     * @see <a href="https://json-stat.org/format/#size">json-stat.org/format/#size</a>
     */
    public ImmutableList<Integer> getSize() {
        return getDimension()
                .values()
                .stream()
                .map(Dimension::getCategory)
                .map(Dimension.Category::getIndex)
                .map(AbstractCollection::size)
                .collect(MoreCollectors.toImmutableList());
    }

    /**
     * Return the updated time of the dataset.
     *
     * @see <a href="https://json-stat.org/format/#updated">json-stat.org/format/#updated</a>
     */
    public Optional<Instant> getUpdated() {
        //  ISO 8601 format recognized by the Javascript Date.parse method (see ECMA-262 Date Time String Format).
        return Optional.ofNullable(updated);
    }

    /**
     * Return the label of the dataset.
     *
     * @see <a href="https://json-stat.org/format/#label">json-stat.org/format/#label</a>
     */
    public Optional<String> getLabel() {
        return Optional.ofNullable(label);
    }

    /**
     * Return the source of the dataset.
     *
     * @see <a href="https://json-stat.org/format/#source">json-stat.org/format/#source</a>
     */
    public Optional<String> getSource() {
        return Optional.ofNullable(source);
    }

    /**
     * Return the value sorted according to the dimensions of the dataset.
     *
     * @see <a href="https://json-stat.org/format/#value">json-stat.org/format/#value</a>
     */
    public abstract Map<Integer, Number> getValue();

    /**
     * Return the values as tuples.
     * <p>
     * The keys are the dimensions and values their associated values.
     */
    public abstract Map<List<String>, List<Number>> asMap();

    /**
     * Return the dimensions of the dataset.
     *
     * @see Dimension
     * @see <a href="https://json-stat.org/format/#dimension">json-stat.org/format/#dimension</a>
     */
    public abstract Map<String, Dimension> getDimension();

    /**
     * Return the dimensions of the dataset.
     *
     * @see Dimension
     * @see <a href="https://json-stat.org/format/#dimension">json-stat.org/format/#dimension</a>
     */
    @JsonIgnore
    public Map<String, Dimension> getDimension(Collection<String> filter) {
        if (firstNonNull(filter, Collections.emptySet()).isEmpty())
            return Collections.emptyMap();

        return Maps.filterKeys(
                getDimension(),
                Predicates.in(filter)
        );
    }

    /**
     * Utility method that returns a {@link Iterable} of {@link List}s going through the data set
     * row by row and cell by cell, in the order defined by the dimensions.
     */
    @JsonIgnore
    public Collection<List<Number>> getRows() {
        return new AbstractCollection<List<Number>>() {
            @Override
            public Iterator<List<Number>> iterator() {
                Iterator<Number> iterator = getValue().values().iterator();
                return Iterators.paddedPartition(iterator, metricSize());
            }

            @Override
            public int size() {
                return getValue().size() / metricSize();
            }
        };
    }

    public abstract Integer metricSize();

    /**
     * A builder for dataset with defined dimensions.
     */
    static class ValuesBuilder implements DatasetValueBuilder {

        private final ImmutableMap<String, Dimension> dimensions;
        private final ImmutableList<List<String>> indexes;
        private final List<List<String>> indexProduct;

        private final String label;
        private final String source;
        private final Instant updated;
        private final int valueSize;

        ValuesBuilder(
                ImmutableSet<Dimension.Builder> dimensions,
                String label,
                String source,
                Instant updated) {

            // Build the dimensions.
            this.dimensions = dimensions.stream()
                    .collect(MoreCollectors.toImmutableMap(
                            Dimension.Builder::getId,
                            Dimension.Builder::build
                    ));

            this.label = label;
            this.source = source;
            this.updated = updated;

            indexes = this.dimensions.values().stream()
                    .map(Dimension::getCategory)
                    .map(Dimension.Category::getIndex)
                    .filter(dims -> dims.size() > 1)
                    .map(ImmutableCollection::asList)
                    .collect(MoreCollectors.toImmutableList());

            indexProduct = Lists.cartesianProduct(indexes);
            valueSize = Math.max(this.dimensions.size() - this.indexes.size(), 1);
        }

        @Override
        public DatasetBuildable withValues(Collection<List<Number>> values) {
            checkNotNull(values);

            if (values.isEmpty())
                return build(Stream.empty());

            return withValues(values.stream());
        }

        @Override
        public DatasetBuildable withValues(Iterable<List<Number>> values) {
            checkNotNull(values);

            // Optimization.
            if (!values.iterator().hasNext())
                return build(Stream.empty());

            return withValues(StreamSupport.stream(
                    values.spliterator(),
                    false
            ));
        }

        @Override
        public DatasetBuildable withValues(Stream<List<Number>> values) {
            checkNotNull(values);

            if (Stream.empty().equals(values))
                return build(Stream.empty());

            Stream<Map.Entry<Integer, Number>> entryStream = StreamUtils.zipWithIndex(values)
                    .flatMap(tuple -> {
                        Integer dimensionIndex = Math.toIntExact(tuple.getIndex());
                        List<Number> metrics = checkNotNull(tuple.getValue());
                        checkArgument(metrics.size() == valueSize,
                                "The value list size was incorrect, got %s, expected %s", metrics.size(), valueSize);
                        return StreamUtils.zipWithIndex(metrics.stream())
                                .map(metric -> {
                                    Integer metricIndex = Math.toIntExact(metric.getIndex());
                                    return new AbstractMap.SimpleEntry<>(
                                            dimensionIndex * valueSize + metricIndex, metric.getValue());
                                });
                    });

            return build(entryStream);
        }

        @Override
        public DatasetBuildable withMapper(Function<List<String>, List<Number>> mapper) {
            // apply function and unroll.
            return withValues(indexProduct.stream().map(mapper));
        }

        @Override
        public DatasetBuildable withFlatValues(Iterable<Number> values) {
            return withValues(Iterables.partition(values, valueSize));
        }

        @Override
        public ValuesBuilder addTuple(List<String> dimensions, List<Number> values) {
            // TODO:
            return this;
        }

        public DatasetBuildable build(Stream<Map.Entry<Integer, Number>> entries) {

            Map<Integer, Number> values = entries.filter(entry -> entry.getValue() != null).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return new DatasetBuildable() {
                @Override
                public Dataset build() {
                    return new Dataset(label, source, updated) {

                        @Override
                        public Map<Integer, Number> getValue() {
                            return values;
                        }

                        @Override
                        public Map<List<String>, List<Number>> asMap() {
                            final Map<List<String>, List<Number>> map = new AbstractMap<List<String>, List<Number>>() {

                                @Override
                                public List<Number> get(Object key) {
                                    int index = indexProduct.indexOf(key);
                                    return getPoints(index);
                                }

                                private List<Number> getPoints(int index) {
                                    List<Number> points = Lists.newArrayList();
                                    for (int i = index; i < index + valueSize; i++) {
                                        points.add(values.get(i));
                                    }
                                    return Collections.unmodifiableList(points);
                                }

                                @Override
                                public Set<Entry<List<String>, List<Number>>> entrySet() {
                                    return new AbstractSet<Entry<List<String>, List<Number>>>() {
                                        @Override
                                        public Iterator<Entry<List<String>, List<Number>>> iterator() {
                                            return new Iterator<Entry<List<String>, List<Number>>>() {

                                                ListIterator<List<String>> keyIterator = indexProduct.listIterator();

                                                @Override
                                                public boolean hasNext() {
                                                    return keyIterator.hasNext();
                                                }

                                                @Override
                                                public Entry<List<String>, List<Number>> next() {
                                                    List<String> dims = keyIterator.next();
                                                    List<Number> metrics = getPoints(keyIterator.previousIndex());
                                                    return new SimpleEntry<>(
                                                            dims,
                                                            metrics
                                                    );
                                                }
                                            };
                                        }

                                        @Override
                                        public int size() {
                                            return values.size() / valueSize;
                                        }
                                    };
                                }
                            };
                            return map;
                        }

                        @Override
                        public Map<String, Dimension> getDimension() {
                            return dimensions;
                        }

                        @Override
                        public Integer metricSize() {
                            return valueSize;
                        }
                    };
                }
            };
        }

    }

    private static class Builder implements DatasetBuilder {

        private final ImmutableSet.Builder<Dimension.Builder> dimensionBuilders;

        private String label;
        private String source;
        private Instant update;

        private Builder() {
            this.dimensionBuilders = ImmutableSet.builder();
        }

        @Override
        public DatasetBuilder withLabel(final String label) {
            this.label = checkNotNull(label, "label was null");
            return this;
        }

        @Override
        public DatasetBuilder withSource(final String source) {
            this.source = checkNotNull(source, "source was null");
            return this;
        }

        @Override
        public DatasetBuilder updatedAt(final Instant update) {
            this.update = checkNotNull(update, "updated was null");
            return this;
        }

        private DatasetBuilder addDimension(Dimension.Builder dimension) {
            checkNotNull(dimension, "the dimension builder was null");


            if (dimensionBuilders.build().contains(dimension))
                throw new DuplicateDimensionException(
                        String.format("the builder already contains the dimension %s", dimension.toString())
                );

            dimensionBuilders.add(dimension);
            return this;
        }

        @Override
        public DatasetValueBuilder withDimensions(Iterable<Dimension.Builder> values) {
            checkNotNull(values, "dimension builder list was null");
            values.forEach(this::addDimension);
            return this.toValueBuilder();
        }

        @Override
        public DatasetValueBuilder withDimensions(Dimension.Builder... values) {
            checkNotNull(values, "dimension builder list was null");
            return this.withDimensions(Arrays.asList(values));
        }

        ValuesBuilder toValueBuilder() {
            return new ValuesBuilder(this.dimensionBuilders.build(), this.label, this.source, this.update);
        }

    }
}
