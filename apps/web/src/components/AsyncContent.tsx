import { Alert, Spin, Typography } from "antd";
import type { ReactNode } from "react";

const { Text } = Typography;

export interface AsyncContentProps {
  loading: boolean;
  error: string | null;
  isEmpty: boolean;
  emptyText?: string;
  children: ReactNode;
}

export default function AsyncContent(props: AsyncContentProps): JSX.Element {
  const { loading, error, isEmpty, emptyText = "No data", children } = props;
  if (loading) return <Spin />;
  if (error) return <Alert type="error" message="Query failed" description={error} showIcon />;
  if (isEmpty) return <Text type="secondary">{emptyText}</Text>;
  return <>{children}</>;
}
