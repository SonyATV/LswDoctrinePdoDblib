<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\Platforms;
use Doctrine\DBAL\Platforms\SQLServer2008Platform as SQLServer;

class SQLServer2008Platform extends SQLServer
{

    /**
     * {@inheritDoc}
     */
    public function getDateTimeFormatString()
    {
        return 'Y-m-d H:i:s';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzFormatString()
    {
        return $this->getDateTimeFormatString();
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset = null)
    {
        if ($limit === null) {
            return $query;
        }

        $start   = $offset + 1;
        $end     = $offset + $limit;
        $orderBy = stristr($query, 'ORDER BY');
        $query   = preg_replace('/\s+ORDER\s+BY\s+([^\)]*)/', '', $query); //Remove ORDER BY from $query
        $format  = 'SELECT * FROM (%s) AS doctrine_tbl WHERE doctrine_rownum BETWEEN %d AND %d';

        if ( ! $orderBy) {
            //Replace only first occurrence of FROM with OVER to prevent changing FROM also in subqueries.
            $query = preg_replace('/\sFROM\s/i', ', ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS doctrine_rownum FROM ', $query, 1);

            return sprintf($format, $query, $start, $end);
        }

        //Clear ORDER BY
        $orderBy        = preg_replace('/ORDER\s+BY\s+([^\)]*)(.*)/', '$1', $orderBy);
        $orderByParts   = explode(',', $orderBy);
        $orderbyColumns = array();

        //Split ORDER BY into parts
        foreach ($orderByParts as &$part) {

            if (preg_match('/(([^\s]*)\.)?([^\.\s]*)\s*(ASC|DESC)?/i', trim($part), $matches)) {
                $orderbyColumns[] = array(
                    'column'    => $matches[3],
                    'hasTable'  => ( ! empty($matches[2])),
                    'sort'      => isset($matches[4]) ? $matches[4] : null,
                    'table'     => empty($matches[2]) ? '[^\.\s]*' : $matches[2]
                );
            }
        }

        $isWrapped = (preg_match('/SELECT DISTINCT .* FROM \(.*\) dctrn_result/', $query)) ? true : false;

        //Find alias for each colum used in ORDER BY
        if ( ! empty($orderbyColumns)) {
            foreach ($orderbyColumns as $column) {

                $pattern    = sprintf('/%s\.%s\s+(?:AS\s+)?([^,\s)]+)/i', $column['table'], $column['column']);

                if ($isWrapped) {
                    $overColumn = preg_match($pattern, $query, $matches)
                        ? $matches[1] : '';
                } else {
                    $overColumn = preg_match($pattern, $query, $matches)
                        ? ($column['hasTable'] ? $column['table']  . '.' : '') . $column['column']
                        : $column['column'];
                }

                if (isset($column['sort'])) {
                    $overColumn .= ' ' . $column['sort'];
                }

                $overColumns[] = $overColumn;
            }
        }

        //Replace only first occurrence of FROM with $over to prevent changing FROM also in subqueries.
        $over  = 'ORDER BY ' . implode(', ', $overColumns);
        $query = preg_replace('/\sFROM\s/i', ", ROW_NUMBER() OVER ($over) AS doctrine_rownum FROM ", $query, 1);

        return sprintf($format, $query, $start, $end);
    }
}